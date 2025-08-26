import json
import logging
from typing import Dict, List
from datetime import datetime
from config import Config
from utils import (
    setup_logging, send_to_sqs, format_incident_for_sqs, 
    get_last_check_time, update_last_check_time, health_check_components
)
from servicenow_client import servicenow_client
from database import db_manager
from exceptions import ServiceNowError, ConfigurationError

logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """
    AWS Lambda handler for fetching incidents from ServiceNow
    
    This function:
    1. Fetches new incidents from ServiceNow for configured assignment groups
    2. Sends incident details to SQS for processing
    3. Updates last check timestamp
    """
    
    # Setup logging
    setup_logging()
    
    # Validate configuration
    config_errors = Config.validate_config()
    if config_errors:
        logger.error(f"Configuration errors: {config_errors}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Configuration validation failed',
                'details': config_errors
            })
        }
    
    try:
        # Initialize database if needed
        db_manager.create_tables()
        
        # Perform health checks
        health_status = health_check_components()
        if not all(health_status.values()):
            logger.error(f"Health check failures: {health_status}")
            return {
                'statusCode': 503,
                'body': json.dumps({
                    'error': 'Service health check failed',
                    'health_status': health_status
                })
            }
        
        # Get last check time
        last_check = get_last_check_time('incident-fetcher')
        if not last_check:
            logger.error("Failed to get last check time")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to get last check time'})
            }
        
        logger.info(f"Fetching incidents since: {last_check.isoformat()}")
        
        # Fetch new incidents from ServiceNow
        incidents = fetch_new_incidents(Config.ASSIGNMENT_GROUPS, last_check)
        
        if not incidents:
            logger.info("No new incidents found")
            # Update last check time even if no incidents found
            update_last_check_time('incident-fetcher', datetime.now())
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No new incidents found',
                    'incidents_processed': 0
                })
            }
        
        # Process and send incidents to SQS
        processed_count = process_incidents_to_sqs(incidents)
        
        # Update last check time
        current_time = datetime.now()
        if update_last_check_time('incident-fetcher', current_time):
            logger.info(f"Updated last check time to: {current_time.isoformat()}")
        
        # Log processing summary
        db_manager.log_processing(
            incident_number="BATCH",
            assignment_group_id="ALL",
            stage="FETCH",
            level="INFO",
            message=f"Successfully processed {processed_count} incidents to SQS",
            details=json.dumps({
                'total_incidents': len(incidents),
                'processed_incidents': processed_count,
                'assignment_groups': Config.ASSIGNMENT_GROUPS,
                'last_check_time': last_check.isoformat()
            })
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Incidents fetched and queued successfully',
                'incidents_found': len(incidents),
                'incidents_processed': processed_count,
                'last_check_time': last_check.isoformat()
            })
        }
        
    except ServiceNowError as e:
        logger.error(f"ServiceNow error: {str(e)}")
        return {
            'statusCode': 502,
            'body': json.dumps({
                'error': 'ServiceNow integration failed',
                'details': str(e)
            })
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in incident fetcher: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'details': str(e)
            })
        }

def fetch_new_incidents(assignment_groups: List[str], last_check_time: datetime) -> List[Dict]:
    """Fetch new incidents from ServiceNow for specified assignment groups"""
    
    try:
        logger.info(f"Fetching incidents for {len(assignment_groups)} assignment groups")
        
        # Fetch incidents from ServiceNow
        incidents = servicenow_client.get_new_incidents(assignment_groups, last_check_time)
        
        if incidents:
            logger.info(f"Retrieved {len(incidents)} new incidents from ServiceNow")
            
            # Log details about retrieved incidents
            priority_counts = {}
            group_counts = {}
            
            for incident in incidents:
                priority = incident.get('priority', 'Unknown')
                priority_counts[priority] = priority_counts.get(priority, 0) + 1
                
                # Extract group name from assignment_group object
                group_name = "Unknown"
                if incident.get('assignment_group'):
                    if isinstance(incident['assignment_group'], dict):
                        group_name = incident['assignment_group'].get('display_value', 'Unknown')
                    else:
                        group_name = str(incident['assignment_group'])
                
                group_counts[group_name] = group_counts.get(group_name, 0) + 1
            
            logger.info(f"Incident breakdown - Priorities: {priority_counts}, Groups: {group_counts}")
        
        return incidents
        
    except ServiceNowError as e:
        logger.error(f"Failed to fetch incidents from ServiceNow: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching incidents: {str(e)}")
        raise ServiceNowError(f"Incident fetch failed: {str(e)}")

def process_incidents_to_sqs(incidents: List[Dict]) -> int:
    """Process incidents and send them to SQS queue"""
    
    processed_count = 0
    
    for incident in incidents:
        try:
            # Extract assignment group name
            assignment_group = extract_assignment_group_name(incident)
            
            if not assignment_group:
                logger.warning(f"Skipping incident {incident.get('number', 'Unknown')} - no assignment group")
                continue
            
            # Format incident for SQS
            sqs_message = format_incident_for_sqs(incident, assignment_group)
            
            # Validate incident data
            if not validate_incident_data(sqs_message):
                logger.warning(f"Skipping incident {incident.get('number', 'Unknown')} - invalid data")
                continue
            
            # Send to SQS
            if send_to_sqs(Config.SQS_QUEUE_URL, sqs_message):
                processed_count += 1
                logger.debug(f"Sent incident {sqs_message['incident_number']} to SQS")
                
                # Log processing step
                db_manager.log_processing(
                    incident_number=sqs_message['incident_number'],
                    assignment_group_id=assignment_group,
                    stage="FETCH",
                    level="DEBUG",
                    message="Incident sent to SQS for processing",
                    details=json.dumps(sqs_message)
                )
            else:
                logger.error(f"Failed to send incident {incident.get('number', 'Unknown')} to SQS")
                
        except Exception as e:
            logger.error(f"Error processing incident {incident.get('number', 'Unknown')}: {str(e)}")
            continue
    
    logger.info(f"Successfully processed {processed_count} out of {len(incidents)} incidents")
    return processed_count

def extract_assignment_group_name(incident: Dict) -> str:
    """Extract assignment group name from incident data"""
    assignment_group = incident.get('assignment_group')
    
    if not assignment_group:
        return ""
    
    # Handle different formats of assignment_group field
    if isinstance(assignment_group, dict):
        # ServiceNow reference object
        return assignment_group.get('display_value', assignment_group.get('value', ''))
    elif isinstance(assignment_group, str):
        # Direct string value
        return assignment_group
    else:
        logger.warning(f"Unexpected assignment_group format: {type(assignment_group)}")
        return str(assignment_group)

def validate_incident_data(incident_data: Dict) -> bool:
    """Validate that incident data contains required fields"""
    required_fields = ['incident_number', 'assignment_group', 'priority', 'opened_at']
    
    for field in required_fields:
        if not incident_data.get(field):
            logger.error(f"Missing required field in incident data: {field}")
            return False
    
    # Validate priority is in expected format
    valid_priorities = ['1', '2', '3', '4', '5']
    if incident_data.get('priority') not in valid_priorities:
        logger.warning(f"Unexpected priority value: {incident_data.get('priority')}")
    
    return True

# For local testing
if __name__ == "__main__":
    # Mock context for local testing
    class MockContext:
        def __init__(self):
            self.function_name = "incident-fetcher"
            self.function_version = "1"
            self.memory_limit_in_mb = 128
    
    # Mock event
    test_event = {}
    
    # Run the handler
    result = lambda_handler(test_event, MockContext())
    print(json.dumps(result, indent=2))
