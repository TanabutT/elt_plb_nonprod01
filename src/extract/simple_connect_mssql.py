import os
import logging
from google.cloud import storage
import pyodbc
import pandas as pd
import io
import json
from datetime import timezone, timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Configuration from environment variables
PROJECT_ID = os.getenv("PROJECT_ID", "terra-mhesi-dp-poc")
GCS_BUCKET = os.getenv("GCS_BUCKET", "terra-mhesi-dp-poc-gcs-bronze-01")
GCS_PREFIX = os.getenv("GCS_PREFIX", "sql-exports-parquet/")
SQL_SERVER_IP = os.getenv("SQL_SERVER_IP", "10.2.1.9")
DB_NAME = os.getenv("DB_NAME", "Master")
DB_USER = os.getenv("DB_USER", "sqlserver")
DB_PASSWORD = os.getenv("DB_PASSWORD", ".Sm6L1Alf{jtvESA") # get it from secret manager
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))  # Parallel table processing

# Query to get all user tables
GET_TABLES_QUERY = """
    SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS full_table_name
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
"""

# Target dev databases list
TARGET_DEV_DATABASES = [
    'dev-auth-service',
    'dev-career-service',
    'dev-data-protection-service',
    'dev-digital-credential-service',
    'dev-document-service',
    'dev-learning-service',
    'dev-notification-service',
    'dev-portfolio-service',
    'dev-question-bank-service'
]

def get_pyodbc_connection(database_name=None):
    """Create and return a pyodbc database connection with proper data type handling.
    
    Args:
        database_name: Optional database name to connect to. Defaults to DB_NAME.
    """
    if database_name is None:
        database_name = DB_NAME
    
    # Create connection string for pyodbc
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER_IP};"
        f"DATABASE={database_name};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        f"Encrypt=no;"
        f"TrustServerCertificate=yes"
    )
    
    # Establish connection using pyodbc
    conn = pyodbc.connect(conn_str)
    
    # Add output converter for DATETIMEOFFSET and other unsupported types
    # SQL type -155 is DATETIMEOFFSET
    def handle_datetimeoffset(value):
        if value is None:
            return None
        try:
            return value.decode('utf-16le')
        except (UnicodeDecodeError, AttributeError):
            # If decoding fails, return as string representation
            return str(value)
    
    conn.add_output_converter(-155, handle_datetimeoffset)
    
    return conn

if __name__ == "__main__":
    get_pyodbc_connection(database_name=None)