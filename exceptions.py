class IncidentAssignmentError(Exception):
    """Base exception for incident assignment system"""
    pass

class ServiceNowError(IncidentAssignmentError):
    """Exception for ServiceNow API related errors"""
    pass

class DatabaseError(IncidentAssignmentError):
    """Exception for database related errors"""
    pass

class WeightCalculationError(IncidentAssignmentError):
    """Exception for weight calculation errors"""
    pass

class ConfigurationError(IncidentAssignmentError):
    """Exception for configuration related errors"""
    pass

class NoAvailableMembersError(IncidentAssignmentError):
    """Exception when no members are available for assignment"""
    pass
