import os
from typing import List

class Config:
    """Configuration class for the ServiceNow incident assignment system"""
    
    # ServiceNow Configuration
    SERVICENOW_INSTANCE = os.getenv('SERVICENOW_INSTANCE', 'your-instance.service-now.com')
    SERVICENOW_USERNAME = os.getenv('SERVICENOW_USERNAME', '')
    SERVICENOW_PASSWORD = os.getenv('SERVICENOW_PASSWORD', '')
    
    # AWS Configuration
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL', '')
    
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', '')
    DB_PORT = int(os.getenv('DB_PORT', '3306'))
    DB_NAME = os.getenv('DB_NAME', 'incident_assignment')
    DB_USER = os.getenv('DB_USER', '')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    
    # Assignment Groups to Monitor (comma-separated)
    ASSIGNMENT_GROUPS = os.getenv('ASSIGNMENT_GROUPS', '').split(',') if os.getenv('ASSIGNMENT_GROUPS') else []
    
    # Weight Calculation Parameters
    WORKLOAD_WEIGHT = float(os.getenv('WORKLOAD_WEIGHT', '0.4'))
    ROLE_WEIGHT = float(os.getenv('ROLE_WEIGHT', '0.3'))
    AVAILABILITY_WEIGHT = float(os.getenv('AVAILABILITY_WEIGHT', '0.3'))
    
    # Priority Weights for Incident Scoring
    PRIORITY_WEIGHTS = {
        '1': 5.0,  # Critical
        '2': 3.0,  # High
        '3': 2.0,  # Medium
        '4': 1.0,  # Low
        '5': 0.5   # Planning
    }
    
    # Role Experience Multipliers
    ROLE_MULTIPLIERS = {
        'L3': 1.5,
        'L2': 1.2,
        'L1': 1.0,
        'TRAINEE': 0.8
    }
    
    # Logging Level
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    @classmethod
    def validate_config(cls) -> List[str]:
        """Validate required configuration parameters"""
        errors = []
        
        required_params = [
            ('SERVICENOW_INSTANCE', cls.SERVICENOW_INSTANCE),
            ('SERVICENOW_USERNAME', cls.SERVICENOW_USERNAME),
            ('SERVICENOW_PASSWORD', cls.SERVICENOW_PASSWORD),
            ('SQS_QUEUE_URL', cls.SQS_QUEUE_URL),
            ('DB_HOST', cls.DB_HOST),
            ('DB_USER', cls.DB_USER),
            ('DB_PASSWORD', cls.DB_PASSWORD)
        ]
        
        for param_name, param_value in required_params:
            if not param_value:
                errors.append(f"Missing required configuration: {param_name}")
        
        if not cls.ASSIGNMENT_GROUPS:
            errors.append("No assignment groups configured")
            
        return errors
