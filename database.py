import pymysql
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from typing import Generator, List, Optional
import logging
from config import Config
from models import Base, MemberData, AssignmentHistory, ProcessingLog
from exceptions import DatabaseError

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self._initialize_engine()
    
    def _initialize_engine(self):
        """Initialize the database engine with connection pooling"""
        try:
            # Use PostgreSQL URL from environment if available, otherwise use MySQL config
            db_url = os.getenv('DATABASE_URL')
            if not db_url:
                # Construct MySQL database URL
                db_url = (
                    f"mysql+pymysql://{Config.DB_USER}:{Config.DB_PASSWORD}@"
                    f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
                )
            
            # Create engine with connection pooling
            self.engine = create_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=300,
                echo=False
            )
            
            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            logger.info("Database engine initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database engine: {str(e)}")
            raise DatabaseError(f"Database initialization failed: {str(e)}")
    
    def create_tables(self):
        """Create all tables if they don't exist"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created/verified successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            raise DatabaseError(f"Table creation failed: {str(e)}")
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a database session with automatic cleanup"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {str(e)}")
            raise DatabaseError(f"Database operation failed: {str(e)}")
        finally:
            session.close()
    
    def get_members_by_group(self, assignment_group_id: str) -> List[MemberData]:
        """Get all active members for a specific assignment group"""
        with self.get_session() as session:
            members = session.query(MemberData).filter(
                MemberData.assignment_group_id == assignment_group_id,
                MemberData.is_active == True
            ).all()
            
            logger.info(f"Retrieved {len(members)} members for group {assignment_group_id}")
            return members
    
    def get_member_by_id(self, member_id: str) -> Optional[MemberData]:
        """Get a specific member by ID"""
        with self.get_session() as session:
            member = session.query(MemberData).filter(
                MemberData.member_id == member_id,
                MemberData.is_active == True
            ).first()
            
            return member
    
    def log_assignment(self, assignment_data: dict):
        """Log an assignment decision to the history table"""
        with self.get_session() as session:
            assignment = AssignmentHistory(**assignment_data)
            session.add(assignment)
            session.flush()
            
            logger.info(f"Assignment logged: {assignment_data['incident_number']} -> {assignment_data['assigned_member_id']}")
    
    def log_processing(self, incident_number: str, assignment_group_id: str, 
                      stage: str, level: str, message: str, details: str = None):
        """Log processing information for debugging and tracing"""
        with self.get_session() as session:
            log_entry = ProcessingLog(
                incident_number=incident_number,
                assignment_group_id=assignment_group_id,
                processing_stage=stage,
                log_level=level,
                message=message,
                details=details
            )
            session.add(log_entry)
            session.flush()
    
    def get_assignment_history(self, member_id: str, days: int = 30) -> List[AssignmentHistory]:
        """Get assignment history for a member within specified days"""
        with self.get_session() as session:
            from datetime import datetime, timedelta
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            history = session.query(AssignmentHistory).filter(
                AssignmentHistory.assigned_member_id == member_id,
                AssignmentHistory.assignment_timestamp >= cutoff_date
            ).all()
            
            return history
    
    def health_check(self) -> bool:
        """Perform a database health check"""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.error(f"Database health check failed: {str(e)}")
            return False

# Global database manager instance
db_manager = DatabaseManager()
