import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
from config import Config
from utils import (
    setup_logging, receive_from_sqs, delete_sqs_message, parse_sqs_message,
    filter_available_members, create_assignment_log_entry, health_check_components
)
from servicenow_client import servicenow_client
from database import db_manager
from weight_calculator import weight_calculator
from exceptions import (
    ServiceNowError, DatabaseError, WeightCalculationError, 
    NoAvailableMembersError, ConfigurationError
)

logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """
    AWS Lambda handler for processing incident assignments
    
    This function:
    1. Receives incident messages from SQS
    2. Processes assignments using the weight-based algorithm
    3. Assigns incidents to the most suitable team members
    4. Logs assignment decisions for transparency
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
        
        # Process messages from SQS
        processing_results = process_sqs_messages()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Assignment processing completed',
                'results': processing_results
            })
        }
        
    except Exception as e:
        logger.error(f"Unexpected error in assignment processor: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'details': str(e)
            })
        }

def process_sqs_messages() -> Dict:
    """Process all available messages from SQS queue"""
    
    results = {
        'messages_processed': 0,
        'assignments_successful': 0,
        'assignments_failed': 0,
        'errors': []
    }
    
    # Receive messages from SQS (process up to 10 messages per invocation)
    messages = receive_from_sqs(Config.SQS_QUEUE_URL, max_messages=10, wait_time=20)
    
    if not messages:
        logger.info("No messages received from SQS")
        return results
    
    logger.info(f"Processing {len(messages)} messages from SQS")
    
    for message in messages:
        try:
            # Parse message
            incident_data = parse_sqs_message(message)
            if not incident_data:
                logger.error("Failed to parse SQS message")
                results['errors'].append("Invalid message format")
                continue
            
            results['messages_processed'] += 1
            
            # Process assignment
            success = process_incident_assignment(incident_data)
            
            if success:
                results['assignments_successful'] += 1
                # Delete message from SQS after successful processing
                delete_sqs_message(Config.SQS_QUEUE_URL, message['ReceiptHandle'])
                logger.debug(f"Deleted processed message for incident {incident_data.get('incident_number')}")
            else:
                results['assignments_failed'] += 1
                # Note: Message will be returned to queue for retry
                
        except Exception as e:
            logger.error(f"Error processing SQS message: {str(e)}")
            results['assignments_failed'] += 1
            results['errors'].append(str(e))
    
    logger.info(f"Processing summary: {results}")
    return results

def process_incident_assignment(incident_data: Dict) -> bool:
    """Process assignment for a single incident"""
    
    incident_number = incident_data.get('incident_number')
    assignment_group = incident_data.get('assignment_group')
    
    logger.info(f"Processing assignment for incident {incident_number}")
    
    try:
        # Log processing start
        db_manager.log_processing(
            incident_number=incident_number,
            assignment_group_id=assignment_group,
            stage="ASSIGN",
            level="INFO",
            message="Starting assignment processing",
            details=json.dumps(incident_data)
        )
        
        # Step 1: Get group members from ServiceNow
        servicenow_members = get_group_members_from_servicenow(assignment_group)
        if not servicenow_members:
            logger.warning(f"No members found in ServiceNow for group {assignment_group}")
            return False
        
        # Step 2: Get member data from database and filter available members
        available_members = get_available_members(assignment_group, servicenow_members)
        if not available_members:
            raise NoAvailableMembersError(f"No available members for group {assignment_group}")
        
        # Step 3: Get current workload for available members
        member_workloads = get_member_workloads(available_members)
        
        # Step 4: Calculate weights for all members
        member_weights = weight_calculator.calculate_member_weights(
            available_members, 
            member_workloads, 
            incident_data.get('priority', '4')
        )
        
        # Step 5: Select best member
        selected_member, assignment_reason = weight_calculator.select_best_member(member_weights)
        
        # Step 6: Assign incident in ServiceNow
        servicenow_member = find_servicenow_member(servicenow_members, selected_member.member_id)
        if not servicenow_member:
            logger.error(f"Could not find ServiceNow user for member {selected_member.member_id}")
            return False
        
        assignment_success = assign_incident_to_member(
            incident_number, 
            servicenow_member['user']['sys_id']
        )
        
        if assignment_success:
            # Step 7: Log assignment decision
            log_assignment_decision(
                incident_data, 
                selected_member, 
                member_weights, 
                assignment_reason
            )
            
            logger.info(f"Successfully assigned incident {incident_number} to {selected_member.member_name}")
            return True
        else:
            logger.error(f"Failed to assign incident {incident_number} in ServiceNow")
            return False
            
    except NoAvailableMembersError as e:
        logger.warning(f"No available members for incident {incident_number}: {str(e)}")
        db_manager.log_processing(
            incident_number=incident_number,
            assignment_group_id=assignment_group,
            stage="ASSIGN",
            level="WARNING",
            message="No available members for assignment",
            details=str(e)
        )
        return False
        
    except WeightCalculationError as e:
        logger.error(f"Weight calculation failed for incident {incident_number}: {str(e)}")
        db_manager.log_processing(
            incident_number=incident_number,
            assignment_group_id=assignment_group,
            stage="CALCULATE",
            level="ERROR",
            message="Weight calculation failed",
            details=str(e)
        )
        return False
        
    except ServiceNowError as e:
        logger.error(f"ServiceNow error for incident {incident_number}: {str(e)}")
        db_manager.log_processing(
            incident_number=incident_number,
            assignment_group_id=assignment_group,
            stage="ASSIGN",
            level="ERROR",
            message="ServiceNow assignment failed",
            details=str(e)
        )
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error processing incident {incident_number}: {str(e)}")
        db_manager.log_processing(
            incident_number=incident_number,
            assignment_group_id=assignment_group,
            stage="ASSIGN",
            level="ERROR",
            message="Unexpected processing error",
            details=str(e)
        )
        return False

def get_group_members_from_servicenow(assignment_group: str) -> List[Dict]:
    """Get group members from ServiceNow"""
    try:
        members = servicenow_client.get_group_members(assignment_group)
        logger.info(f"Retrieved {len(members)} members from ServiceNow for group {assignment_group}")
        return members
    except ServiceNowError as e:
        logger.error(f"Failed to get group members from ServiceNow: {str(e)}")
        raise

def get_available_members(assignment_group: str, servicenow_members: List[Dict]) -> List:
    """Get available members from database and filter by availability"""
    try:
        # Get all members for the group from database
        db_members = db_manager.get_members_by_group(assignment_group)
        
        if not db_members:
            logger.warning(f"No members found in database for group {assignment_group}")
            return []
        
        # Filter to only include members that exist in both ServiceNow and database
        available_members = []
        servicenow_member_ids = {member['user']['user_name'] for member in servicenow_members}
        
        for db_member in db_members:
            if db_member.member_id in servicenow_member_ids:
                available_members.append(db_member)
            else:
                logger.debug(f"Member {db_member.member_id} not found in ServiceNow group")
        
        # Filter by current availability (shift, weekend, etc.)
        current_time = datetime.now()
        filtered_members = []
        
        for member in available_members:
            if is_member_currently_available(member, current_time):
                filtered_members.append(member)
            else:
                logger.debug(f"Member {member.member_id} not currently available")
        
        logger.info(f"Found {len(filtered_members)} available members for assignment")
        return filtered_members
        
    except DatabaseError as e:
        logger.error(f"Database error getting available members: {str(e)}")
        raise

def is_member_currently_available(member, current_time: datetime) -> bool:
    """Check if a member is currently available based on shift and other criteria"""
    # Check if it's weekend and member doesn't work weekends
    is_weekend = current_time.weekday() >= 5
    if is_weekend and not member.weekend_shift_flag:
        return False
    
    # Check if current time is within shift hours
    try:
        from datetime import time
        shift_start = time.fromisoformat(member.shift_start)
        shift_end = time.fromisoformat(member.shift_end)
        current_time_only = current_time.time()
        
        if shift_start <= shift_end:
            # Normal shift (doesn't cross midnight)
            in_shift = shift_start <= current_time_only <= shift_end
        else:
            # Night shift (crosses midnight)
            in_shift = current_time_only >= shift_start or current_time_only <= shift_end
        
        # Allow some flexibility (1 hour before/after shift)
        if not in_shift:
            # Calculate proximity to shift
            from weight_calculator import WeightCalculator
            calculator = WeightCalculator()
            availability_score = calculator._calculate_availability_score(member)
            # If availability score is very low, consider unavailable
            if availability_score < 0.3:
                return False
        
        return True
        
    except ValueError:
        logger.warning(f"Invalid shift time format for member {member.member_id}")
        return True  # Default to available if shift format is invalid

def get_member_workloads(available_members: List) -> Dict[str, List[Dict]]:
    """Get current workload (active incidents) for all available members"""
    member_workloads = {}
    
    for member in available_members:
        try:
            # Get ServiceNow user to find sys_id
            user = servicenow_client.get_user_by_username(member.member_id)
            if not user:
                logger.warning(f"Could not find ServiceNow user for {member.member_id}")
                member_workloads[member.member_id] = []
                continue
            
            # Get current incidents for this member
            incidents = servicenow_client.get_member_incidents(user['sys_id'])
            member_workloads[member.member_id] = incidents
            
            logger.debug(f"Member {member.member_id} has {len(incidents)} active incidents")
            
        except ServiceNowError as e:
            logger.error(f"Failed to get workload for member {member.member_id}: {str(e)}")
            member_workloads[member.member_id] = []
    
    return member_workloads

def find_servicenow_member(servicenow_members: List[Dict], member_id: str) -> Optional[Dict]:
    """Find ServiceNow member data by member ID"""
    for member in servicenow_members:
        if member['user']['user_name'] == member_id:
            return member
    return None

def assign_incident_to_member(incident_number: str, member_sys_id: str) -> bool:
    """Assign incident to member in ServiceNow"""
    try:
        return servicenow_client.assign_incident(incident_number, member_sys_id)
    except ServiceNowError as e:
        logger.error(f"Failed to assign incident {incident_number}: {str(e)}")
        return False

def log_assignment_decision(incident_data: Dict, selected_member, 
                          member_weights: List, assignment_reason: str):
    """Log the assignment decision with full transparency"""
    try:
        # Create weight summary for logging
        weight_summary = weight_calculator.get_weights_summary(member_weights)
        
        # Create assignment log entry
        assignment_log = create_assignment_log_entry(
            incident_data, 
            selected_member, 
            weight_summary, 
            assignment_reason
        )
        
        # Log to database
        db_manager.log_assignment(assignment_log)
        
        # Log processing completion
        db_manager.log_processing(
            incident_number=incident_data['incident_number'],
            assignment_group_id=incident_data['assignment_group'],
            stage="ASSIGN",
            level="INFO",
            message="Assignment completed successfully",
            details=json.dumps({
                'assigned_member': selected_member.member_id,
                'final_weight': selected_member.final_weight,
                'assignment_reason': assignment_reason,
                'total_candidates': len(member_weights)
            })
        )
        
        logger.info(f"Assignment decision logged for incident {incident_data['incident_number']}")
        
    except DatabaseError as e:
        logger.error(f"Failed to log assignment decision: {str(e)}")
        # Don't fail the assignment if logging fails

# For local testing
if __name__ == "__main__":
    # Mock context for local testing
    class MockContext:
        def __init__(self):
            self.function_name = "assignment-processor"
            self.function_version = "1"
            self.memory_limit_in_mb = 256
    
    # Mock event
    test_event = {}
    
    # Run the handler
    result = lambda_handler(test_event, MockContext())
    print(json.dumps(result, indent=2))
