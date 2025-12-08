import os
import logging
from google.cloud import storage
import pyodbc
import pandas as pd
import io
import json
from datetime import timezone, timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv, find_dotenv
from google.cloud import secretmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Find the correct .env file based on the current environment
env_file = find_dotenv(f'.env')
# print('env_file: ', env_file)

# Load the environment variables from the .env file
load_dotenv(env_file)


PROJECT_ID = os.getenv("PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_PREFIX = os.getenv("GCS_PREFIX")
SQL_SERVER_IP = os.getenv("SQL_SERVER_IP")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")

# Access Secret Manager to retrieve the database password
def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")
SECRET_NAME = os.getenv("SECRET_NAME")
DB_PASSWORD = get_secret(SECRET_NAME)
MAX_WORKERS = int(os.getenv("MAX_WORKERS"))

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