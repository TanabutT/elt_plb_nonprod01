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
PROJECT_ID = os.getenv("PROJECT_ID", "poc-piloturl-nonprod")
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


def get_table_list():
    """Fetch list of all tables from the database."""
    logger.info("Fetching table list from database")
    conn = get_pyodbc_connection()
    cursor = conn.cursor()
    
    cursor.execute(GET_TABLES_QUERY)
    tables = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    logger.info(f"Found {len(tables)} tables")
    return tables


def export_table_to_gcs(table_name, database_name=None):
    """
    Export a single table to GCS as parquet using pandas DataFrame.
    Uses the successful approach from onetable.py to handle all data types properly.
    FIXED: Now creates empty parquet files for tables with no data instead of skipping them.
    
    Args:
        table_name: Name of the table to export
        database_name: Optional database name. If not provided, uses DB_NAME
    """
    # Use default database if none provided
    if database_name is None:
        database_name = DB_NAME
        logger.info(f"Starting export for table: {table_name}")
    else:
        logger.info(f"Exporting {database_name}.{table_name} as parquet")
    
    try:
        # Use pyodbc connection for better data type handling
        conn = get_pyodbc_connection(database_name)
        cursor = conn.cursor()
        
        # Get schema and table name
        schema = table_name.split('.')[0] if '.' in table_name else 'dbo'
        table = table_name.split('.')[-1]
        
        # Construct SQL query
        SQL_QUERY = f"SELECT * FROM [{database_name}].{schema}.[{table}]"
        
        # Execute query and fetch data
        cursor.execute(SQL_QUERY)
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        
        # Create DataFrame
        df = pd.DataFrame.from_records(data, columns=columns)
        
        cursor.close()
        conn.close()
        
        # Initialize GCS client and blob
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET)
        
        # Sanitize table name for filename
        safe_table_name = table_name.replace('.', '_').replace(' ', '_')
        
        # Create date folder and parquet-specific folder structure
        bangkok_tz = timezone(timedelta(hours=7))
        date_folder = datetime.now(bangkok_tz).strftime("%Y%m%d")
        format_folder = "parquetextract"
        file_extension = "parquet"
        
        # Different blob path if database_name is provided (for dev databases)
        if database_name != DB_NAME:
            blob_name = f"{GCS_PREFIX}{date_folder}/{format_folder}/{database_name}/{safe_table_name}.{file_extension}"
        else:
            blob_name = f"{GCS_PREFIX}{date_folder}/{format_folder}/{safe_table_name}.{file_extension}"
            
        blob = bucket.blob(blob_name)
        
        # FIXED: Create parquet file even if DataFrame is empty
        # This ensures all tables have corresponding parquet files
        parquet_buffer = io.BytesIO()
        
        if df.empty:
            logger.warning(f"No data found for table {table_name}, creating empty parquet file")
            # Create empty DataFrame with correct column structure
            df_empty = pd.DataFrame(columns=columns)
            df_empty.to_parquet(parquet_buffer, index=False)
            row_count = 0
        else:
            df.to_parquet(parquet_buffer, index=False)
            row_count = len(df)
        
        # Upload to GCS
        parquet_buffer.seek(0)
        blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
        
        gcs_path = f"gs://{GCS_BUCKET}/{blob_name}"
        
        if database_name == DB_NAME:
            logger.info(f"Successfully exported {table_name}: {row_count} rows to {gcs_path}")
            return {
                "table": table_name,
                "status": "success",
                "rows": row_count,
                "gcs_path": gcs_path
            }
        else:
            logger.info(f"Successfully exported {database_name}.{table_name}: {row_count} rows to {gcs_path}")
            return {
                "database": database_name,
                "table": table_name,
                "format": "parquet",
                "status": "success",
                "rows": row_count,
                "gcs_path": gcs_path
            }
        
    except Exception as e:
        logger.error(f"Error exporting table {table_name}: {str(e)}")
        if database_name == DB_NAME:
            return {
                "table": table_name,
                "status": "failed",
                "error": str(e)
            }
        else:
            return {
                "database": database_name,
                "table": table_name,
                "format": "parquet",
                "status": "failed",
                "error": str(e)
            }
        
def export_dev_databases():
    """
    Export all tables from all dev databases as parquet.
    This will export from all 9 dev-* databases.
    FIXED: Now creates empty parquet files for tables with no data.
    """
    try:
        # Target dev databases
        target_databases = TARGET_DEV_DATABASES

        logger.info(f"Starting parquet export for {len(target_databases)} dev databases")

        all_results = {}
        total_databases = len(target_databases)
        successful_databases = 0
        failed_databases = 0
        total_tables_exported = 0
        total_rows_exported = 0

        for db_name in target_databases:
            logger.info(f"Exporting database: {db_name}")

            try:
                # Connect to the specific database
                conn = get_pyodbc_connection(db_name)
                cursor = conn.cursor()

                # Get tables from this database
                cursor.execute(GET_TABLES_QUERY)
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()

                logger.info(f"Found {len(tables)} tables in {db_name}")

                if not tables:
                    all_results[db_name] = {
                        "status": "skipped",
                        "message": "No tables found",
                        "tables_exported": 0,
                        "rows_exported": 0
                    }
                    continue

                # Export each table from this database
                db_results = []
                db_successful = 0
                db_failed = 0
                db_rows = 0

                for table in tables:
                    try:
                        result = export_table_to_gcs(table, db_name)
                        db_results.append(result)

                        if result["status"] == "success":
                            db_successful += 1
                            db_rows += result.get("rows", 0)
                        else:
                            db_failed += 1

                    except Exception as e:
                        logger.error(f"Error exporting {db_name}.{table}: {str(e)}")
                        db_results.append({
                            "database": db_name,
                            "table": table,
                            "status": "failed",
                            "error": str(e)
                        })
                        db_failed += 1

                all_results[db_name] = {
                    "status": "completed",
                    "tables_total": len(tables),
                    "tables_successful": db_successful,
                    "tables_failed": db_failed,
                    "rows_exported": db_rows,
                    "results": db_results
                }

                successful_databases += 1
                total_tables_exported += db_successful
                total_rows_exported += db_rows

                logger.info(f"Completed {db_name}: {db_successful}/{len(tables)} tables, {db_rows} rows")

            except Exception as e:
                logger.error(f"Error processing database {db_name}: {str(e)}")
                all_results[db_name] = {
                    "status": "failed",
                    "error": str(e)
                }
                failed_databases += 1

        logger.info(f"All dev databases parquet export completed: {successful_databases}/{total_databases} databases, {total_tables_exported} tables, {total_rows_exported} rows")

        # Create JSON response
        response_data = {
            "status": "completed",
            "format": "parquet",
            "summary": {
                "total_databases": total_databases,
                "successful_databases": successful_databases,
                "failed_databases": failed_databases,
                "total_tables_exported": total_tables_exported,
                "total_rows_exported": total_rows_exported
            },
            "databases": all_results
        }

        # Write results to GCS
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET)
        date_folder = datetime.now().strftime("%Y%m%d")
        blob_name = f"{GCS_PREFIX}{date_folder}/export_results.json"
        blob = bucket.blob(blob_name)
        
        blob.upload_from_string(
            json.dumps(response_data, indent=2),
            content_type='application/json'
        )

        logger.info(f"Export results written to gs://{GCS_BUCKET}/{blob_name}")

        return jsonify(response_data), 200
    except Exception as e:
        logger.error(f"Dev databases export job failed: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
