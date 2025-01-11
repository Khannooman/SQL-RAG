from sqlalchemy import create_engine, MetaData, inspect, Engine
from sqlalchemy.orm import sessionmaker
from typing import Tuple, Optional, Dict, Any
from enum import Enum
import urllib
import logging


class DatabaseType(Enum):
    """Supported database types with SQLAlchemy Connection prefix"""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql+pymysql"
    ORACLE = "oracle+cx_oracle"
    MSSQL = "mssql+pyodbc"
    SQLITE = "sqlite"

class DatabaseConnection:
    """universal database connection manager for different RDBMS type"""
    def __init__(self,
                 db_type: DatabaseType,
                 username: Optional[str] = None, 
                 password: Optional[str] = None, 
                 host: Optional[str] = None, 
                 port: Optional[str] = None, 
                 database: Optional[str] = None,
                 connection_params: Optional[Dict[str, Any]] = None
                 ):
        """Initialize parameter

            Args:
                db_type: DatabaseType enum specifying the type of database
                username: username of database
                password: password of thedatabase
                host = host of database (where database is running)
                port = in which port database is running
                database = name of the database
        """

        self.db_type = db_type
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection_params = connection_params or None
        self.engine = None
        self.session = None
        self.metadata = None

    def _build_connection_string(self) -> str:
        """build the appropirate connection string based on the datatype"""
        if self.db_type ==DatabaseType.SQLITE:
            return f"{self.db_type.value}:///{self.database}"
        
        encode_password = urllib.parse.quote_plus(self.password) if self.password else ''
        ## standard connection string for other database
        base_url = f"{self.db_type.value}://{self.username}:{encode_password}@{self.host}:{self.port}/{self.database}"
        if self.connection_params:
            params = "&".join(f"{k}={v}" for k, v in self.connection_params.items())
            base_url = f"{base_url}?{params}"
        
        return base_url
    
    def connect(self) -> Tuple[Engine, sessionmaker, MetaData]:
        """Establish database connection"""

        try:
            #create engine
            database_connection_string=self._build_connection_string()
            self.engine = create_engine(database_connection_string)

            # Create session factory and metadata
            self.session = sessionmaker(bind=self.engine)
            self.metadata = MetaData()

            logging.info(f"Successfully connected to {self.db_type.value} database")
            return self.engine, self.session, self.metadata
        
        except Exception as e:
            error_mssg = f"Failed to connect with {self.db_type.value} database {str({e})}"
            logging.error(error_mssg)

        

    def test_connection(self) -> bool:
        """Test if database connection is working"""
        if not self.engine:
            return False       
        try:
            with self.engine.connect() as connection:
                connection.execute("SELECT 1")
            return True
        except Exception as e:
            logging.error(f"Connection test failed: {str(e)}")
            return False

    def close(self) -> None:
        """Close database connection and dispose of engine"""
        if self.engine:
            self.engine.dispose()
            logging.info("Database connection closed")





    