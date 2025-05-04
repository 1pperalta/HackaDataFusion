#!/usr/bin/env python
"""
Snowflake-S3 Connector for GitHub Archive Gold Data

This script connects Snowflake to an AWS S3 bucket containing GitHub Archive gold data.
It sets up a storage integration, file format, external stage, and creates tables in Snowflake.

Environment variables or .env file should contain Snowflake and AWS credentials.
"""

import os
import sys
import logging
import argparse
import json
import datetime
from pathlib import Path
from typing import Dict, List, Any
import time

import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/snowflake_s3_connector.log", mode='a')
    ]
)
logger = logging.getLogger("snowflake_s3_connector")

# Create logs directory if it doesn't exist
Path("logs").mkdir(exist_ok=True)

# Load environment variables
load_dotenv()

# Default gold tables to create in Snowflake
GOLD_TABLES = [
    "actor_metrics",
    "repo_metrics", 
    "org_metrics",
    "event_type_metrics",
    "daily_summary",
    "geographical_activity",
    "repository_popularity",
    "time_based_activity"
]

# Table schemas for gold tables
TABLE_SCHEMAS = {
    "actor_metrics": """
        actor_id NUMBER,
        total_events NUMBER,
        unique_repos NUMBER,
        first_event TIMESTAMP_NTZ,
        last_event TIMESTAMP_NTZ,
        actor_login VARCHAR,
        is_bot BOOLEAN
    """,
    "repo_metrics": """
        repo_id NUMBER,
        total_events NUMBER,
        unique_actors NUMBER,
        first_event TIMESTAMP_NTZ,
        last_event TIMESTAMP_NTZ,
        repo_name VARCHAR,
        language VARCHAR
    """,
    "org_metrics": """
        org_id NUMBER,
        total_events NUMBER,
        unique_actors NUMBER,
        first_event TIMESTAMP_NTZ,
        last_event TIMESTAMP_NTZ,
        org_login VARCHAR
    """,
    "event_type_metrics": """
        event_type VARCHAR,
        count NUMBER
    """,
    "daily_summary": """
        hour_bucket VARCHAR,
        total_events NUMBER,
        unique_actors NUMBER,
        unique_repos NUMBER
    """,
    "geographical_activity": """
        country VARCHAR,
        event_count NUMBER,
        actor_count NUMBER,
        date DATE
    """,
    "repository_popularity": """
        repo_id NUMBER,
        repo_name VARCHAR,
        stars_count NUMBER,
        forks_count NUMBER,
        watchers_count NUMBER,
        date DATE
    """,
    "time_based_activity": """
        hour_of_day NUMBER,
        event_count NUMBER,
        date DATE
    """
}


class SnowflakeS3Connector:
    """
    Manages connection between Snowflake and S3 bucket with GitHub Archive Gold data
    """
    
    def __init__(
        self,
        snowflake_account: str,
        snowflake_user: str,
        snowflake_password: str,
        snowflake_warehouse: str,
        snowflake_database: str,
        snowflake_schema: str,
        snowflake_role: str,
        s3_bucket_name: str,
        s3_region: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        s3_path_prefix: str = "github-archive/gold"
    ):
        """
        Initialize the Snowflake S3 connector
        
        Args:
            snowflake_account: Snowflake account identifier
            snowflake_user: Snowflake username
            snowflake_password: Snowflake password
            snowflake_warehouse: Snowflake warehouse name
            snowflake_database: Snowflake database name
            snowflake_schema: Snowflake schema name
            snowflake_role: Snowflake role name
            s3_bucket_name: Name of the S3 bucket containing gold data
            s3_region: AWS region of the S3 bucket
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            s3_path_prefix: Path prefix within the bucket for gold data
        """
        self.snowflake_account = snowflake_account
        self.snowflake_user = snowflake_user
        self.snowflake_password = snowflake_password
        self.snowflake_warehouse = snowflake_warehouse
        self.snowflake_database = snowflake_database
        self.snowflake_schema = snowflake_schema
        self.snowflake_role = snowflake_role
        
        self.s3_bucket_name = s3_bucket_name
        self.s3_region = s3_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_path_prefix = s3_path_prefix.rstrip('/')
        
        self.conn = None
        self.cursor = None
        self.integration_name = "S3_GITHUB_ARCHIVE_INTEGRATION"
        self.stage_name = "GITHUB_ARCHIVE_GOLD_STAGE"
        self.file_format_name = "GITHUB_PARQUET_FORMAT"
    
    def connect_to_snowflake(self) -> bool:
        """
        Establish connection to Snowflake
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            self.conn = snowflake.connector.connect(
                user=self.snowflake_user,
                password=self.snowflake_password,
                account=self.snowflake_account,
                warehouse=self.snowflake_warehouse,
                database=self.snowflake_database,
                schema=self.snowflake_schema,
                role=self.snowflake_role
            )
            
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to Snowflake account: {self.snowflake_account}")
            logger.info(f"Using warehouse: {self.snowflake_warehouse}, database: {self.snowflake_database}, schema: {self.snowflake_schema}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def close_connection(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")
    
    def execute_query(self, query: str, params: Dict = None, silent: bool = False) -> List[Dict]:
        """
        Execute a query in Snowflake
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            silent: Whether to suppress logging the query
            
        Returns:
            List of dictionaries with query results
        """
        try:
            if not silent:
                logger.info(f"Executing query: {query}")
            
            self.cursor.execute(query, params)
            
            # For queries that return results
            if self.cursor.description:
                columns = [col[0] for col in self.cursor.description]
                rows = self.cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            
            return []
            
        except ProgrammingError as e:
            # Handle specific known errors
            error_code = getattr(e, 'errno', None)
            
            # Handle "object already exists" errors gracefully
            if error_code == 2002:  # Object already exists
                logger.warning(f"Object already exists: {e}")
                return []
            else:
                logger.error(f"Error executing query: {e}")
                logger.error(f"Query: {query}")
                raise
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def create_database_if_not_exists(self) -> bool:
        """
        Create the database if it doesn't exist
        
        Returns:
            True if successful, False otherwise
        """
        try:
            query = f"""
            CREATE DATABASE IF NOT EXISTS {self.snowflake_database}
            """
            
            self.execute_query(query)
            logger.info(f"Database {self.snowflake_database} exists or was created")
            
            # Use the database
            query = f"""
            USE DATABASE {self.snowflake_database}
            """
            self.execute_query(query)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            return False
    
    def create_schema_if_not_exists(self) -> bool:
        """
        Create the schema if it doesn't exist
        
        Returns:
            True if successful, False otherwise
        """
        try:
            query = f"""
            CREATE SCHEMA IF NOT EXISTS {self.snowflake_schema}
            """
            
            self.execute_query(query)
            logger.info(f"Schema {self.snowflake_schema} exists or was created")
            
            # Use the schema
            query = f"""
            USE SCHEMA {self.snowflake_schema}
            """
            self.execute_query(query)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            return False
    
    def create_storage_integration(self) -> bool:
        """
        Create a storage integration to connect to S3
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if integration already exists
            query = f"""
            SHOW INTEGRATIONS LIKE '{self.integration_name}'
            """
            result = self.execute_query(query, silent=True)
            
            if result:
                logger.info(f"Storage integration {self.integration_name} already exists")
                return True
            
            # Create storage integration
            query = f"""
            CREATE STORAGE INTEGRATION {self.integration_name}
              TYPE = EXTERNAL_STAGE
              STORAGE_PROVIDER = 'S3'
              ENABLED = TRUE
              STORAGE_AWS_ROLE_ARN = ''
              STORAGE_ALLOWED_LOCATIONS = ('s3://{self.s3_bucket_name}/{self.s3_path_prefix}/')
              STORAGE_AWS_EXTERNAL_ID = ''
              STORAGE_AWS_IAM_USER_ARN = ''
              COMMENT = 'Integration for GitHub Archive Gold data in S3'
            """
            
            self.execute_query(query)
            logger.info(f"Created storage integration {self.integration_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create storage integration: {e}")
            return False
    
    def create_file_format(self) -> bool:
        """
        Create file format for parquet files
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if file format already exists
            query = f"""
            SHOW FILE FORMATS LIKE '{self.file_format_name}'
            """
            result = self.execute_query(query, silent=True)
            
            if result:
                logger.info(f"File format {self.file_format_name} already exists")
                return True
            
            # Create file format for parquet files
            query = f"""
            CREATE FILE FORMAT {self.file_format_name}
              TYPE = 'PARQUET'
            """
            
            self.execute_query(query)
            logger.info(f"Created file format {self.file_format_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create file format: {e}")
            return False
    
    def create_stage(self) -> bool:
        """
        Create external stage connecting to S3
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if stage already exists
            query = f"""
            SHOW STAGES LIKE '{self.stage_name}'
            """
            result = self.execute_query(query, silent=True)
            
            if result:
                logger.info(f"Stage {self.stage_name} already exists")
                return True
            
            # Create external stage
            query = f"""
            CREATE STAGE {self.stage_name}
              URL = 's3://{self.s3_bucket_name}/{self.s3_path_prefix}/'
              CREDENTIALS = (AWS_KEY_ID = '{self.aws_access_key_id}' AWS_SECRET_KEY = '{self.aws_secret_access_key}')
              FILE_FORMAT = {self.file_format_name}
            """
            
            self.execute_query(query)
            logger.info(f"Created external stage {self.stage_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create external stage: {e}")
            return False
    
    def create_table(self, table_name: str) -> bool:
        """
        Create a table in Snowflake based on the predefined schema
        
        Args:
            table_name: Name of the table to create
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if table already exists
            query = f"""
            SHOW TABLES LIKE '{table_name}'
            """
            result = self.execute_query(query, silent=True)
            
            if result:
                logger.info(f"Table {table_name} already exists")
                return True
            
            # Get the schema for this table
            schema = TABLE_SCHEMAS.get(table_name)
            if not schema:
                logger.error(f"No schema defined for table {table_name}")
                return False
            
            # Create table with schema
            query = f"""
            CREATE TABLE {table_name} (
              {schema}
            )
            COMMENT = 'GitHub Archive Gold data - {table_name}'
            """
            
            self.execute_query(query)
            logger.info(f"Created table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def list_s3_files(self, table_name: str) -> List[str]:
        """
        List files in S3 for a specific table
        
        Args:
            table_name: Name of the table to list files for
            
        Returns:
            List of file paths in the stage
        """
        try:
            query = f"""
            LIST @{self.stage_name}/{table_name}/
            """
            
            result = self.execute_query(query)
            files = [item.get('name', '') for item in result if 'name' in item]
            
            logger.info(f"Found {len(files)} files for table {table_name}")
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files for {table_name}: {e}")
            return []
    
    def load_data_from_stage(self, table_name: str) -> int:
        """
        Load data from stage into Snowflake table
        
        Args:
            table_name: Name of the table to load data into
            
        Returns:
            Number of rows loaded
        """
        try:
            # Use COPY INTO command to load data
            query = f"""
            COPY INTO {table_name}
            FROM @{self.stage_name}/{table_name}/
            FILE_FORMAT = (FORMAT_NAME = {self.file_format_name})
            PATTERN = '.*\.parquet'
            ON_ERROR = 'CONTINUE'
            """
            
            self.execute_query(query)
            
            # Check how many rows were loaded
            query = f"""
            SELECT COUNT(*) as row_count FROM {table_name}
            """
            
            result = self.execute_query(query)
            row_count = result[0].get('ROW_COUNT', 0) if result else 0
            
            logger.info(f"Loaded {row_count} rows into table {table_name}")
            return row_count
            
        except Exception as e:
            logger.error(f"Failed to load data into {table_name}: {e}")
            return 0
    
    def setup_and_load_table(self, table_name: str) -> Dict[str, Any]:
        """
        Set up a table and load data from S3
        
        Args:
            table_name: Name of the table to set up and load
            
        Returns:
            Dictionary with setup results
        """
        results = {
            "table_name": table_name,
            "success": False,
            "rows_loaded": 0,
            "errors": []
        }
        
        try:
            # Check for files in S3
            files = self.list_s3_files(table_name)
            
            if not files:
                logger.warning(f"No files found for {table_name}, skipping")
                results["errors"].append(f"No files found in S3 for {table_name}")
                return results
            
            # Create table
            table_created = self.create_table(table_name)
            
            if not table_created:
                results["errors"].append(f"Failed to create table {table_name}")
                return results
            
            # Load data from S3 into table
            rows_loaded = self.load_data_from_stage(table_name)
            
            results["success"] = True
            results["rows_loaded"] = rows_loaded
            
            return results
            
        except Exception as e:
            logger.error(f"Error setting up and loading table {table_name}: {e}")
            results["errors"].append(str(e))
            return results
    
    def setup_all_tables(self, tables: List[str] = None) -> Dict[str, Any]:
        """
        Set up all gold tables and load data from S3
        
        Args:
            tables: List of table names to set up, or None for all tables
            
        Returns:
            Dictionary with setup results
        """
        tables = tables or GOLD_TABLES
        results = {
            "tables_configured": 0,
            "rows_loaded": 0,
            "table_results": {}
        }
        
        for table_name in tables:
            logger.info(f"Setting up table {table_name}")
            table_result = self.setup_and_load_table(table_name)
            
            results["table_results"][table_name] = table_result
            
            if table_result["success"]:
                results["tables_configured"] += 1
                results["rows_loaded"] += table_result["rows_loaded"]
        
        return results
    
    def run(self, tables: List[str] = None) -> Dict[str, Any]:
        """
        Run the full setup process
        
        Args:
            tables: List of table names to set up, or None for all tables
            
        Returns:
            Dictionary with run results
        """
        start_time = datetime.datetime.now()
        results = {
            "success": False,
            "tables_configured": 0,
            "rows_loaded": 0,
            "table_results": {},
            "errors": [],
            "duration_seconds": 0
        }
        
        try:
            # Connect to Snowflake
            connected = self.connect_to_snowflake()
            if not connected:
                results["errors"].append("Failed to connect to Snowflake")
                return results
            
            # Create database and schema if they don't exist
            db_created = self.create_database_if_not_exists()
            if not db_created:
                results["errors"].append("Failed to create database")
                self.close_connection()
                return results
            
            schema_created = self.create_schema_if_not_exists()
            if not schema_created:
                results["errors"].append("Failed to create schema")
                self.close_connection()
                return results
            
            # Create file format
            format_created = self.create_file_format()
            if not format_created:
                results["errors"].append("Failed to create file format")
                self.close_connection()
                return results
            
            # Create storage integration (requires admin privileges)
            try:
                integration_created = self.create_storage_integration()
                if not integration_created:
                    logger.warning("Storage integration creation skipped or failed")
            except Exception as e:
                logger.warning(f"Storage integration error: {e}, proceeding with direct credentials")
            
            # Create external stage
            stage_created = self.create_stage()
            if not stage_created:
                results["errors"].append("Failed to create external stage")
                self.close_connection()
                return results
            
            # Set up tables and load data
            setup_results = self.setup_all_tables(tables)
            results.update(setup_results)
            
            # Set success flag
            results["success"] = True
            
        except Exception as e:
            logger.error(f"Error running Snowflake S3 connector: {e}")
            results["errors"].append(str(e))
            
        finally:
            # Close Snowflake connection
            self.close_connection()
            
            # Calculate duration
            end_time = datetime.datetime.now()
            results["duration_seconds"] = (end_time - start_time).total_seconds()
            
            return results


def parse_arguments():
    """
    Parse command line arguments
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Connect Snowflake to S3 for GitHub Archive Gold data"
    )
    
    parser.add_argument(
        "--tables",
        type=str,
        nargs="+",
        help="List of tables to set up (default: all gold tables)"
    )
    
    return parser.parse_args()


def main():
    """Main function"""
    try:
        # Create logs directory if it doesn't exist
        Path("logs").mkdir(exist_ok=True)
        
        # Parse arguments
        args = parse_arguments()
        
        # Get credentials from environment variables
        snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
        snowflake_user = os.getenv("SNOWFLAKE_USER")
        snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
        snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        snowflake_database = os.getenv("SNOWFLAKE_DATABASE", "GITHUB_DATA")
        snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA", "BRONZE")
        snowflake_role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        s3_bucket_name = os.getenv("AWS_S3_BUCKET")
        s3_region = os.getenv("AWS_REGION", "us-east-2")
        
        # Validate required credentials
        missing_creds = []
        if not snowflake_account: missing_creds.append("SNOWFLAKE_ACCOUNT")
        if not snowflake_user: missing_creds.append("SNOWFLAKE_USER")
        if not snowflake_password: missing_creds.append("SNOWFLAKE_PASSWORD")
        if not aws_access_key_id: missing_creds.append("AWS_ACCESS_KEY_ID")
        if not aws_secret_access_key: missing_creds.append("AWS_SECRET_ACCESS_KEY")
        if not s3_bucket_name: missing_creds.append("AWS_S3_BUCKET")
        
        if missing_creds:
            print(f"❌ Missing environment variables: {', '.join(missing_creds)}")
            print("Please set these environment variables or create a .env file")
            return 1
        
        # Initialize connector
        connector = SnowflakeS3Connector(
            snowflake_account=snowflake_account,
            snowflake_user=snowflake_user,
            snowflake_password=snowflake_password,
            snowflake_warehouse=snowflake_warehouse,
            snowflake_database=snowflake_database,
            snowflake_schema=snowflake_schema,
            snowflake_role=snowflake_role,
            s3_bucket_name=s3_bucket_name,
            s3_region=s3_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            s3_path_prefix="github-archive/gold"
        )
        
        # Run connector
        results = connector.run(tables=args.tables)
        
        # Print results
        if results["success"]:
            print(f"\n✅ Snowflake S3 connector completed successfully")
            print(f"📊 {results['tables_configured']} tables configured")
            print(f"🔢 {results['rows_loaded']} total rows loaded")
            print(f"⏱️ Duration: {results['duration_seconds']:.2f} seconds")
            
            # Print details for each table
            print("\nTable details:")
            for table_name, result in results["table_results"].items():
                status = "✅" if result["success"] else "❌"
                print(f"  {status} {table_name}: {result['rows_loaded']} rows loaded")
                
                if result["errors"]:
                    for error in result["errors"]:
                        print(f"    ⚠️ {error}")
            
            return 0
            
        else:
            print(f"\n❌ Snowflake S3 connector failed")
            for error in results["errors"]:
                print(f"  ⚠️ {error}")
            
            return 1
        
    except Exception as e:
        logger.critical(f"Error in Snowflake S3 connector: {e}", exc_info=True)
        print(f"\n❌ Error in Snowflake S3 connector: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())