import requests
from typing import List, Dict, Optional
import logging
from datetime import datetime, timedelta
import json
from config import Config
from exceptions import ServiceNowError

logger = logging.getLogger(__name__)

class ServiceNowClient:
    """Client for interacting with ServiceNow REST API"""
    
    def __init__(self):
        self.base_url = f"https://{Config.SERVICENOW_INSTANCE}"
        self.auth = (Config.SERVICENOW_USERNAME, Config.SERVICENOW_PASSWORD)
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
    def _make_request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict:
        """Make an authenticated request to ServiceNow API"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.request(
                method=method,
                url=url,
                auth=self.auth,
                headers=self.headers,
                params=params,
                json=data,
                timeout=30
            )
            
            response.raise_for_status()
            
            if response.status_code == 204:  # No content
                return {}
                
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"ServiceNow API request failed: {str(e)}")
            raise ServiceNowError(f"API request failed: {str(e)}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse ServiceNow response: {str(e)}")
            raise ServiceNowError(f"Invalid JSON response: {str(e)}")
    
    def get_new_incidents(self, assignment_groups: List[str], 
                         last_check_time: Optional[datetime] = None) -> List[Dict]:
        """Fetch new incidents for specified assignment groups"""
        if not assignment_groups:
            logger.warning("No assignment groups specified")
            return []
        
        # Build query parameters
        params = {
            'sysparm_query': self._build_incident_query(assignment_groups, last_check_time),
            'sysparm_fields': 'number,assignment_group,priority,severity,opened_at,short_description,state,assigned_to',
            'sysparm_limit': 1000
        }
        
        logger.info(f"Fetching incidents for groups: {assignment_groups}")
        
        response = self._make_request('GET', '/api/now/table/incident', params=params)
        incidents = response.get('result', [])
        
        logger.info(f"Retrieved {len(incidents)} new incidents")
        return incidents
    
    def _build_incident_query(self, assignment_groups: List[str], 
                            last_check_time: Optional[datetime] = None) -> str:
        """Build ServiceNow query for incident retrieval"""
        # Filter by assignment groups
        group_conditions = []
        for group in assignment_groups:
            group_conditions.append(f"assignment_group.name={group}")
        
        group_query = "^OR".join(group_conditions)
        
        # Filter by state (not closed/resolved)
        state_query = "state!=6^state!=7^state!=8"  # Exclude Resolved, Closed, Canceled
        
        # Filter by time if provided
        time_query = ""
        if last_check_time:
            time_str = last_check_time.strftime('%Y-%m-%d %H:%M:%S')
            time_query = f"^opened_at>={time_str}"
        
        # Combine all conditions
        query = f"({group_query})^{state_query}{time_query}"
        
        logger.debug(f"ServiceNow query: {query}")
        return query
    
    def get_group_members(self, assignment_group_name: str) -> List[Dict]:
        """Get members of a specific assignment group"""
        params = {
            'sysparm_query': f'name={assignment_group_name}',
            'sysparm_fields': 'sys_id,name'
        }
        
        # First get the group sys_id
        response = self._make_request('GET', '/api/now/table/sys_user_group', params=params)
        groups = response.get('result', [])
        
        if not groups:
            logger.warning(f"Assignment group not found: {assignment_group_name}")
            return []
        
        group_sys_id = groups[0]['sys_id']
        
        # Now get group members
        params = {
            'sysparm_query': f'group={group_sys_id}',
            'sysparm_fields': 'user.sys_id,user.user_name,user.name,user.email'
        }
        
        response = self._make_request('GET', '/api/now/table/sys_user_grmember', params=params)
        members = response.get('result', [])
        
        logger.info(f"Retrieved {len(members)} members for group {assignment_group_name}")
        return members
    
    def get_member_incidents(self, member_sys_id: str) -> List[Dict]:
        """Get current active incidents assigned to a member"""
        params = {
            'sysparm_query': f'assigned_to={member_sys_id}^state!=6^state!=7^state!=8',
            'sysparm_fields': 'number,priority,severity,opened_at,state'
        }
        
        response = self._make_request('GET', '/api/now/table/incident', params=params)
        incidents = response.get('result', [])
        
        logger.debug(f"Member {member_sys_id} has {len(incidents)} active incidents")
        return incidents
    
    def assign_incident(self, incident_number: str, member_sys_id: str) -> bool:
        """Assign an incident to a specific member"""
        # First get the incident sys_id
        params = {
            'sysparm_query': f'number={incident_number}',
            'sysparm_fields': 'sys_id'
        }
        
        response = self._make_request('GET', '/api/now/table/incident', params=params)
        incidents = response.get('result', [])
        
        if not incidents:
            logger.error(f"Incident not found: {incident_number}")
            return False
        
        incident_sys_id = incidents[0]['sys_id']
        
        # Update the incident with assignment
        update_data = {
            'assigned_to': member_sys_id,
            'state': '2',  # In Progress
            'work_notes': f'Auto-assigned by intelligent assignment system at {datetime.now().isoformat()}'
        }
        
        try:
            self._make_request('PUT', f'/api/now/table/incident/{incident_sys_id}', data=update_data)
            logger.info(f"Successfully assigned incident {incident_number} to {member_sys_id}")
            return True
        except ServiceNowError as e:
            logger.error(f"Failed to assign incident {incident_number}: {str(e)}")
            return False
    
    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Get user details by username"""
        params = {
            'sysparm_query': f'user_name={username}',
            'sysparm_fields': 'sys_id,user_name,name,email'
        }
        
        response = self._make_request('GET', '/api/now/table/sys_user', params=params)
        users = response.get('result', [])
        
        return users[0] if users else None
    
    def health_check(self) -> bool:
        """Perform a health check on ServiceNow connection"""
        try:
            params = {'sysparm_limit': 1}
            self._make_request('GET', '/api/now/table/incident', params=params)
            return True
        except ServiceNowError:
            return False

# Global ServiceNow client instance
servicenow_client = ServiceNowClient()
