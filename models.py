from sqlalchemy import Column, String, Integer, DateTime, Boolean, Text, Index, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional

Base = declarative_base()

class MemberData(Base):
    """Model for storing member data and availability information"""
    __tablename__ = 'member_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    assignment_group_id = Column(String(100), nullable=False, index=True)
    member_id = Column(String(100), nullable=False, index=True)
    member_name = Column(String(200), nullable=False)
    role = Column(String(50), nullable=False)  # L1, L2, L3, TRAINEE
    experience_level = Column(Integer, default=1)  # Years of experience
    shift_start = Column(String(10), nullable=False)  # Format: HH:MM (24-hour)
    shift_end = Column(String(10), nullable=False)    # Format: HH:MM (24-hour)
    weekend_shift_flag = Column(Boolean, default=False)
    timezone = Column(String(50), default='UTC')
    is_active = Column(Boolean, default=True)
    created_date = Column(DateTime, default=func.now())
    updated_date = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Composite index for efficient queries
    __table_args__ = (
        Index('idx_group_member', 'assignment_group_id', 'member_id'),
        Index('idx_active_members', 'assignment_group_id', 'is_active'),
    )
    
    def __repr__(self):
        return f"<MemberData(member_id='{self.member_id}', name='{self.member_name}', role='{self.role}')>"

class AssignmentHistory(Base):
    """Model for tracking assignment history and decisions"""
    __tablename__ = 'assignment_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    incident_number = Column(String(100), nullable=False, index=True)
    assignment_group_id = Column(String(100), nullable=False)
    assigned_member_id = Column(String(100), nullable=False)
    assigned_member_name = Column(String(200), nullable=False)
    incident_priority = Column(String(10), nullable=False)
    incident_severity = Column(String(10))
    assignment_timestamp = Column(DateTime, default=func.now())
    
    # Weight calculation details for transparency
    calculated_weights = Column(Text)  # JSON string of all member weights
    workload_score = Column(Numeric(10, 4))
    role_score = Column(Numeric(10, 4))
    availability_score = Column(Numeric(10, 4))
    final_weight = Column(Numeric(10, 4))
    
    assignment_reason = Column(Text)  # Human-readable reason for assignment
    created_date = Column(DateTime, default=func.now())
    
    def __repr__(self):
        return f"<AssignmentHistory(incident='{self.incident_number}', assigned_to='{self.assigned_member_id}')>"

class ProcessingLog(Base):
    """Model for detailed processing logs and debugging"""
    __tablename__ = 'processing_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    incident_number = Column(String(100), index=True)
    assignment_group_id = Column(String(100))
    processing_stage = Column(String(100), nullable=False)  # FETCH, FILTER, CALCULATE, ASSIGN
    log_level = Column(String(20), default='INFO')  # DEBUG, INFO, WARNING, ERROR
    message = Column(Text, nullable=False)
    details = Column(Text)  # JSON string for additional details
    timestamp = Column(DateTime, default=func.now())
    
    def __repr__(self):
        return f"<ProcessingLog(incident='{self.incident_number}', stage='{self.processing_stage}')>"
