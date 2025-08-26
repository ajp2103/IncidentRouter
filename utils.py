import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import boto3
from botocore.exceptions import ClientError
from config import Config

logger = logging.getLogger(__name__)

def setup_logging(level: str = None):
    """Setup logging configuration"""
    log_level = level or Config.LOG_LEVEL
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def send_to_sqs(queue_url: str, message_body: Dict, delay_seconds: int = 0) -> bool:
    """Send a message to SQS queue"""
    try:
        sqs = boto3.client('sqs', region_name=Config.AWS_REGION)
        
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body),
            DelaySeconds=delay_seconds
        )
        
        logger.debug(f"Message sent to SQS: {response['MessageId']}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to send message to SQS: {str(e)}")
        return False

def receive_from_sqs(queue_url: str, max_messages: int = 1, wait_time: int = 20) -> List[Dict]:
    """Receive messages from SQS queue"""
    try:
        sqs = boto3.client('sqs', region_name=Config.AWS_REGION)
        
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=['All']
        )
        
        messages = response.get('Messages', [])
        logger.debug(f"Received {len(messages)} messages from SQS")
        
        return messages
        
    except ClientError as e:
        logger.error(f"Failed to receive messages from SQS: {str(e)}")
        return []

def delete_sqs_message(queue_url: str, receipt_handle: str) -> bool:
    """Delete a message from SQS queue"""
    try:
        sqs = boto3.client('sqs', region_name=Config.AWS_REGION)
        
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        
        logger.debug("Message deleted from SQS")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to delete message from SQS: {str(e)}")
        return False

def format_incident_for_sqs(incident: Dict, assignment_group: str) -> Dict:
    """Format incident data for SQS message"""
    return {
        'incident_number': incident.get('number'),
        'assignment_group': assignment_group,
        'priority': incident.get('priority'),
        'severity': incident.get('severity'),
        'opened_at': incident.get('opened_at'),
        'short_description': incident.get('short_description'),
        'state': incident.get('state'),
        'processing_timestamp': datetime.now().isoformat()
    }

def parse_sqs_message(message: Dict) -> Optional[Dict]:
    """Parse SQS message body"""
    try:
        body = json.loads(message['Body'])
        return body
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Failed to parse SQS message: {str(e)}")
        return None

def get_last_check_time(lambda_function_name: str) -> Optional[datetime]:
    """Get the last successful check time from CloudWatch or parameter store"""
    try:
        # Using SSM Parameter Store to track last check time
        ssm = boto3.client('ssm', region_name=Config.AWS_REGION)
        
        parameter_name = f"/incident-assignment/{lambda_function_name}/last-check"
        
        response = ssm.get_parameter(Name=parameter_name)
        timestamp_str = response['Parameter']['Value']
        
        return datetime.fromisoformat(timestamp_str)
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            logger.info("No previous check time found, using default")
            # Return time 1 hour ago as default
            return datetime.now() - timedelta(hours=1)
        else:
            logger.error(f"Failed to get last check time: {str(e)}")
            return None

def update_last_check_time(lambda_function_name: str, timestamp: datetime) -> bool:
    """Update the last successful check time"""
    try:
        ssm = boto3.client('ssm', region_name=Config.AWS_REGION)
        
        parameter_name = f"/incident-assignment/{lambda_function_name}/last-check"
        
        ssm.put_parameter(
            Name=parameter_name,
            Value=timestamp.isoformat(),
            Type='String',
            Overwrite=True
        )
        
        logger.debug(f"Updated last check time: {timestamp.isoformat()}")
        return True
        
    except ClientError as e:
        logger.error(f"Failed to update last check time: {str(e)}")
        return False

def filter_available_members(members: List[Dict], current_time: datetime = None) -> List[Dict]:
    """Filter members based on availability criteria"""
    if current_time is None:
        current_time = datetime.now()
    
    available_members = []
    
    for member in members:
        # Check if member is active
        if not member.get('is_active', True):
            continue
        
        # Check weekend availability
        is_weekend = current_time.weekday() >= 5
        if is_weekend and not member.get('weekend_shift_flag', False):
            continue
        
        # Additional availability checks can be added here
        available_members.append(member)
    
    logger.info(f"Filtered to {len(available_members)} available members from {len(members)} total")
    return available_members

def validate_incident_data(incident_data: Dict) -> bool:
    """Validate incident data contains required fields"""
    required_fields = ['incident_number', 'assignment_group', 'priority', 'opened_at']
    
    for field in required_fields:
        if not incident_data.get(field):
            logger.error(f"Missing required field: {field}")
            return False
    
    return True

def create_assignment_log_entry(incident_data: Dict, selected_member: Any, 
                               weight_details: Dict, assignment_reason: str) -> Dict:
    """Create a complete assignment log entry"""
    return {
        'incident_number': incident_data['incident_number'],
        'assignment_group_id': incident_data['assignment_group'],
        'assigned_member_id': selected_member.member_id,
        'assigned_member_name': selected_member.member_name,
        'incident_priority': incident_data['priority'],
        'incident_severity': incident_data.get('severity'),
        'calculated_weights': json.dumps(weight_details),
        'workload_score': selected_member.workload_score,
        'role_score': selected_member.role_score,
        'availability_score': selected_member.availability_score,
        'final_weight': selected_member.final_weight,
        'assignment_reason': assignment_reason
    }

def get_secret(secret_name: str) -> Optional[str]:
    """Retrieve secret from AWS Secrets Manager"""
    try:
        secrets_client = boto3.client('secretsmanager', region_name=Config.AWS_REGION)
        
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return response['SecretString']
        
    except ClientError as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {str(e)}")
        return None

def health_check_components() -> Dict[str, bool]:
    """Perform health checks on all system components"""
    from database import db_manager
    from servicenow_client import servicenow_client
    
    health_status = {}
    
    # Database health check
    try:
        health_status['database'] = db_manager.health_check()
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        health_status['database'] = False
    
    # ServiceNow health check
    try:
        health_status['servicenow'] = servicenow_client.health_check()
    except Exception as e:
        logger.error(f"ServiceNow health check failed: {str(e)}")
        health_status['servicenow'] = False
    
    # SQS health check
    try:
        sqs = boto3.client('sqs', region_name=Config.AWS_REGION)
        sqs.get_queue_attributes(QueueUrl=Config.SQS_QUEUE_URL)
        health_status['sqs'] = True
    except Exception as e:
        logger.error(f"SQS health check failed: {str(e)}")
        health_status['sqs'] = False
    
    return health_status
