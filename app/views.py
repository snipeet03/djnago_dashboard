from django.shortcuts import render
import pandas as pd
from datetime import datetime, time, timedelta
import cx_Oracle
from sqlalchemy import create_engine, text
import json
from plotly.utils import PlotlyJSONEncoder
import plotly.express as px
import pyodbc
import warnings
import logging
import sys
import os

# Configure logging to capture all output
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'machine_data_debug.log')
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def startup_view(request):
    logger.info("Starting startup_view function")
    warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")

    # Oracle DB credentials (consider using environment variables)
    oracle_username = os.getenv("ORACLE_USERNAME", "SVE1NA")
    oracle_password = os.getenv("ORACLE_PASSWORD", "Wilkommen$131")
    oracle_host = "na0orarac01.apac.bosch.com"
    oracle_port = 38000
    oracle_service = "MFN2PROD_na0orarac01.apac.bosch.com"
    
    # Oracle DSN and engine setup
    dsn = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service)
    oracle_engine = None
    try:
        oracle_engine = create_engine(
            f'oracle+cx_oracle://{oracle_username}:{oracle_password}@{dsn}',
            connect_args={"encoding": "UTF-8"}
        )
        logger.info("Oracle engine created successfully")
    except Exception as e:
        logger.error(f"Failed to create Oracle engine: {str(e)}")

    # SQL Server DB credentials (consider using environment variables)
    sql_server = 'na0vm00024.apac.bosch.com'
    sql_database = 'DB_MFC2DB_SQL'
    sql_username = os.getenv("SQL_USERNAME", 'SVE1NA')
    sql_password = os.getenv("SQL_PASSWORD", 'wILKOMMEN$131')
    
    # SQL Server engine setup
    sql_engine = None
    try:
        sql_engine = create_engine(
            f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        logger.info("SQL Server engine created successfully")
    except Exception as e:
        logger.error(f"Failed to create SQL Server engine: {str(e)}")

    # Get current date and time
    now_local = datetime.now()
    today = now_local.date()
    current_time = now_local.time()
    current_day = now_local.strftime('%A')  # Get current day name
    
    logger.info(f"Current time: {now_local}, Day: {current_day}")

    # Define shift timings and labels
    if time(6, 0) <= current_time < time(14, 0):
        shift_start = datetime.combine(today, time(6, 0))
        shift_end = datetime.combine(today, time(14, 0))
        shift_label = "Shift 1 (06:00 - 14:00 Local)"
    elif time(14, 0) <= current_time < time(22, 0):
        shift_start = datetime.combine(today, time(14, 0))
        shift_end = datetime.combine(today, time(22, 0))
        shift_label = "Shift 2 (14:00 - 22:00 Local)"
    else:
        if current_time >= time(22, 0):
            shift_start = datetime.combine(today, time(22, 0))
            shift_end = datetime.combine(today + timedelta(days=1), time(6, 0))
        else:
            shift_start = datetime.combine(today - timedelta(days=1), time(22, 0))
            shift_end = datetime.combine(today, time(6, 0))
        shift_label = "Shift 3 (22:00 - 06:00 Local)"

    shift_start_str = shift_start.strftime('%Y-%m-%d %H:%M:%S')
    shift_end_str = shift_end.strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Shift details: {shift_label}, Start: {shift_start_str}, End: {shift_end_str}")

    # Calculate net production time in seconds
    net_production_time_seconds = (now_local - shift_start).total_seconds()

    # Define cycle times for each machine (in seconds)
    cycle_times = {
        "Mikron": 5.26,
        "Machine 1": 15,
        "Machine 2": 15,
        "Machine 3": 15,
        "TATP Parameter 1": 15.3,
        "TATP Parameter 2": 15.3
    }

    # Function to fetch SQL Server hourly data using different approaches
    def fetch_sql_server_hourly_data(table_name, shift_start, shift_end, timestamp_column="TimeStamp", part_count_column="PartCount", method="auto"):
        logger.info(f"Fetching SQL Server data for table: {table_name}, method: {method}")
        
        # Create a default result with all hours in the shift and zero values
        expected_hours = pd.date_range(start=shift_start, end=shift_end - timedelta(minutes=1), freq='h')
        df_result = pd.DataFrame({'HOUR': expected_hours})
        df_result['LABEL_HOUR'] = df_result['HOUR'].dt.strftime('%H:00')
        df_result['PART_COUNT'] = 0
        df_result['RECORD_COUNT'] = 0
        
        try:
            # Format dates as strings for SQL Server
            start_str = shift_start.strftime('%Y-%m-%d %H:%M:%S')
            end_str = shift_end.strftime('%Y-%m-%d %H:%M:%S')
            
            # First, check if the table exists and get its columns
            check_query = f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}'
            """
            
            columns_df = pd.read_sql(check_query, sql_engine)
            
            # Check if the timestamp column exists
            if timestamp_column not in columns_df['COLUMN_NAME'].values:
                # Try common alternatives
                alternatives = ['timestamp', 'datetime', 'date', 'time', 'created_at', 'updated_at']
                for alt in alternatives:
                    if alt in columns_df['COLUMN_NAME'].values:
                        timestamp_column = alt
                        logger.info(f"Using alternative timestamp column: {alt}")
                        break
                else:
                    logger.warning(f"No suitable timestamp column found for table {table_name}")
                    return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]
            
            # Check if part count column exists
            if part_count_column not in columns_df['COLUMN_NAME'].values:
                # Try common alternatives
                alternatives = ['part_count', 'parts', 'quantity', 'count']
                for alt in alternatives:
                    if alt in columns_df['COLUMN_NAME'].values:
                        part_count_column = alt
                        logger.info(f"Using alternative part count column: {alt}")
                        break
                else:
                    logger.warning(f"No suitable part count column found for table {table_name}")
                    return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]
            
            # Get all raw data for the shift period
            raw_query = f"""
                SELECT {timestamp_column}, {part_count_column}
                FROM {table_name}
                WHERE {timestamp_column} >= '{start_str}' AND {timestamp_column} < '{end_str}'
                ORDER BY {timestamp_column}
            """
            
            df_raw = pd.read_sql(raw_query, sql_engine)
            logger.info(f"Retrieved {len(df_raw)} raw records from {table_name}")
            
            if df_raw.empty:
                logger.warning(f"No data found for table {table_name} in the specified time range")
                return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]
            
            # Convert timestamp to datetime
            df_raw[timestamp_column] = pd.to_datetime(df_raw[timestamp_column])
            
            # Create a column for the hour
            df_raw['HOUR'] = df_raw[timestamp_column].dt.floor('H')
            
            # Create a DataFrame with all hours in the shift
            expected_hours = pd.date_range(start=shift_start, end=shift_end - timedelta(minutes=1), freq='h')
            df_result = pd.DataFrame({'HOUR': expected_hours})
            
            # Calculate part count based on the selected method
            if method == "auto":
                # Try different methods and pick the one that gives non-zero results
                methods_to_try = ["minmax", "distinct", "changes", "nonzero"]
                best_method = None
                best_result = None
                best_nonzero_hours = 0
                
                for method_name in methods_to_try:
                    if method_name == "minmax":
                        # Min/max approach
                        hourly_data = df_raw.groupby('HOUR').agg({
                            part_count_column: ['min', 'max', 'count']
                        }).reset_index()
                        hourly_data.columns = ['HOUR', 'MIN_PART_COUNT', 'MAX_PART_COUNT', 'RECORD_COUNT']
                        hourly_data['PART_COUNT'] = hourly_data['MAX_PART_COUNT'] - hourly_data['MIN_PART_COUNT']
                        
                        # If negative, set to 0 and warn
                        mask = hourly_data['PART_COUNT'] < 0
                        if mask.any():
                            hourly_data.loc[mask, 'PART_COUNT'] = 0
                            logger.warning(f"Found negative part counts using minmax method, set to 0")
                        
                        method_result = hourly_data[['HOUR', 'PART_COUNT', 'RECORD_COUNT']]
                        
                    elif method_name == "distinct":
                        # Count distinct values approach
                        hourly_data = df_raw.groupby('HOUR').agg({
                            part_count_column: pd.Series.nunique
                        }).reset_index()
                        hourly_data.columns = ['HOUR', 'PART_COUNT']
                        
                        # Get record count separately
                        record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                        
                        method_result = pd.merge(hourly_data, record_counts, on='HOUR')
                        
                    elif method_name == "changes":
                        # Count changes in value approach
                        df_sorted = df_raw.sort_values(timestamp_column)
                        df_sorted['PREV_VALUE'] = df_sorted[part_count_column].shift(1)
                        df_sorted['CHANGED'] = (df_sorted[part_count_column] != df_sorted['PREV_VALUE']).astype(int)
                        
                        # First record always counts as a change
                        df_sorted.loc[df_sorted[timestamp_column] == df_sorted.groupby('HOUR')[timestamp_column].transform('min'), 'CHANGED'] = 1
                        
                        hourly_data = df_sorted.groupby('HOUR').agg({
                            'CHANGED': 'sum'
                        }).reset_index()
                        hourly_data.columns = ['HOUR', 'PART_COUNT']
                        
                        # Get record count separately
                        record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                        
                        method_result = pd.merge(hourly_data, record_counts, on='HOUR')
                        
                    elif method_name == "nonzero":
                        # Count non-zero values approach
                        nonzero_data = df_raw[df_raw[part_count_column] > 0]
                        hourly_data = nonzero_data.groupby('HOUR').size().reset_index(name='PART_COUNT')
                        
                        # Get record count separately
                        record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                        
                        method_result = pd.merge(hourly_data, record_counts, on='HOUR', how='right')
                        method_result['PART_COUNT'] = method_result['PART_COUNT'].fillna(0).astype(int)
                    
                    # Count non-zero hours
                    nonzero_hours = (method_result['PART_COUNT'] > 0).sum()
                    
                    if nonzero_hours > best_nonzero_hours:
                        best_nonzero_hours = nonzero_hours
                        best_method = method_name
                        best_result = method_result
                
                logger.info(f"Selected best method: {best_method} with {best_nonzero_hours} non-zero hours")
                df_result = pd.merge(df_result, best_result, on='HOUR', how='left')
                
            else:
                # Use the specified method
                if method == "minmax":
                    # Min/max approach
                    hourly_data = df_raw.groupby('HOUR').agg({
                        part_count_column: ['min', 'max', 'count']
                    }).reset_index()
                    hourly_data.columns = ['HOUR', 'MIN_PART_COUNT', 'MAX_PART_COUNT', 'RECORD_COUNT']
                    hourly_data['PART_COUNT'] = hourly_data['MAX_PART_COUNT'] - hourly_data['MIN_PART_COUNT']
                    
                    # If negative, set to 0 and warn
                    mask = hourly_data['PART_COUNT'] < 0
                    if mask.any():
                        hourly_data.loc[mask, 'PART_COUNT'] = 0
                        logger.warning(f"Found negative part counts using minmax method, set to 0")
                    
                    df_result = pd.merge(df_result, hourly_data[['HOUR', 'PART_COUNT', 'RECORD_COUNT']], on='HOUR', how='left')
                    
                elif method == "distinct":
                    # Count distinct values approach
                    hourly_data = df_raw.groupby('HOUR').agg({
                        part_count_column: pd.Series.nunique
                    }).reset_index()
                    hourly_data.columns = ['HOUR', 'PART_COUNT']
                    
                    # Get record count separately
                    record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                    
                    df_result = pd.merge(df_result, hourly_data, on='HOUR', how='left')
                    df_result = pd.merge(df_result, record_counts, on='HOUR', how='left')
                    
                elif method == "changes":
                    # Count changes in value approach
                    df_sorted = df_raw.sort_values(timestamp_column)
                    df_sorted['PREV_VALUE'] = df_sorted[part_count_column].shift(1)
                    df_sorted['CHANGED'] = (df_sorted[part_count_column] != df_sorted['PREV_VALUE']).astype(int)
                    
                    # First record always counts as a change
                    df_sorted.loc[df_sorted[timestamp_column] == df_sorted.groupby('HOUR')[timestamp_column].transform('min'), 'CHANGED'] = 1
                    
                    hourly_data = df_sorted.groupby('HOUR').agg({
                        'CHANGED': 'sum'
                    }).reset_index()
                    hourly_data.columns = ['HOUR', 'PART_COUNT']
                    
                    # Get record count separately
                    record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                    
                    df_result = pd.merge(df_result, hourly_data, on='HOUR', how='left')
                    df_result = pd.merge(df_result, record_counts, on='HOUR', how='left')
                    
                elif method == "nonzero":
                    # Count non-zero values approach
                    nonzero_data = df_raw[df_raw[part_count_column] > 0]
                    hourly_data = nonzero_data.groupby('HOUR').size().reset_index(name='PART_COUNT')
                    
                    # Get record count separately
                    record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                    
                    df_result = pd.merge(df_result, hourly_data, on='HOUR', how='right')
                    df_result['PART_COUNT'] = df_result['PART_COUNT'].fillna(0).astype(int)
            
            # Fill missing values with 0
            df_result['PART_COUNT'] = df_result['PART_COUNT'].fillna(0).astype(int)
            df_result['RECORD_COUNT'] = df_result['RECORD_COUNT'].fillna(0).astype(int)
            df_result['LABEL_HOUR'] = df_result['HOUR'].dt.strftime('%H:00')
            
            return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]
            
        except Exception as e:
            logger.error(f"Error fetching SQL Server data for table {table_name}: {str(e)}")
            return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]

    # Query Mikron data from Oracle
    logger.info("Querying Mikron data from Oracle")
    df_actual = pd.DataFrame()
    if oracle_engine:
        try:
            with oracle_engine.connect() as connection:
                connection.execute(text("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"))
                query = """
                    SELECT 
                        TO_CHAR(RESULT_DATE, 'YYYY-MM-DD HH24') AS RESULT_HOUR,
                        COUNT(*) AS RECORD_COUNT
                    FROM PDAECM.LOCATION_RESULTS
                    WHERE LOCATION_ID LIKE :loc_id
                      AND RESULT_DATE >= TO_DATE(:start_time, 'YYYY-MM-DD HH24:MI:SS')
                      AND RESULT_DATE < TO_DATE(:end_time, 'YYYY-MM-DD HH24:MI:SS')
                    GROUP BY TO_CHAR(RESULT_DATE, 'YYYY-MM-DD HH24')
                    ORDER BY RESULT_HOUR
                """
                df_actual = pd.read_sql(
                    query,
                    con=connection,
                    params={
                        "loc_id": "%829%",
                        "start_time": shift_start_str,
                        "end_time": shift_end_str,
                    }
                )
                df_actual.columns = [col.upper() for col in df_actual.columns]
                logger.info(f"Retrieved Mikron data: {len(df_actual)} records")
        except Exception as e:
            logger.error(f"Error querying Mikron data: {str(e)}")

    # Prepare Mikron dataframe
    expected_hours = pd.date_range(start=shift_start, end=shift_end - timedelta(minutes=1), freq='h')
    df_expected = pd.DataFrame({'RESULT_HOUR': expected_hours})

    if not df_actual.empty:
        df_actual['RESULT_HOUR'] = pd.to_datetime(df_actual['RESULT_HOUR'], format='%Y-%m-%d %H')
        df_merged = pd.merge(df_expected, df_actual, on='RESULT_HOUR', how='left')
        df_merged['RECORD_COUNT'] = df_merged['RECORD_COUNT'].fillna(0).astype(int)
    else:
        df_merged = df_expected.copy()
        df_merged['RECORD_COUNT'] = 0

    df_merged['LABEL_HOUR'] = df_merged['RESULT_HOUR'].dt.strftime('%H:00')

    # Create Mikron chart
    fig_mikron = px.bar(
        df_merged,
        x='LABEL_HOUR',
        y='RECORD_COUNT',
        labels={"LABEL_HOUR": "Hour", "RECORD# filepath: c:\Users\navne\Desktop\idhd DashBord\machine_data\app\views.py
from django.shortcuts import render
import pandas as pd
from datetime import datetime, time, timedelta
import cx_Oracle
from sqlalchemy import create_engine, text
import json
from plotly.utils import PlotlyJSONEncoder
import plotly.express as px
import pyodbc
import warnings
import logging
import sys
import os

# Configure logging to capture all output
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'machine_data_debug.log')
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def startup_view(request):
    logger.info("Starting startup_view function")
    warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")

    # Oracle DB credentials (consider using environment variables)
    oracle_username = os.getenv("ORACLE_USERNAME", "SVE1NA")
    oracle_password = os.getenv("ORACLE_PASSWORD", "Wilkommen$131")
    oracle_host = "na0orarac01.apac.bosch.com"
    oracle_port = 38000
    oracle_service = "MFN2PROD_na0orarac01.apac.bosch.com"
    
    # Oracle DSN and engine setup
    dsn = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service)
    oracle_engine = None
    try:
        oracle_engine = create_engine(
            f'oracle+cx_oracle://{oracle_username}:{oracle_password}@{dsn}',
            connect_args={"encoding": "UTF-8"}
        )
        logger.info("Oracle engine created successfully")
    except Exception as e:
        logger.error(f"Failed to create Oracle engine: {str(e)}")

    # SQL Server DB credentials (consider using environment variables)
    sql_server = 'na0vm00024ac.bosch.com'
    sql_database = 'DB_MFC2DB_SQL'
    sql_username = os.getenv("SQL_USERNAME", 'SVE1NA')
    sql_password = os.getenv("SQL_PASSWORD", 'wILKOMMEN$131')
    
    # SQL Server engine setup
    sql_engine = None
    try:
        sql_engine = create_engine(
            f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        logger.info("SQL Server engine created successfully")
    except Exception as e:
        logger.error(f"Failed to create SQL Server engine: {str(e)}")

    # Get current date and time
    now_local = datetime.now()
    today = now_local.date()
    current_time = now_local.time()
    current_day = now_local.strftime('%A')  # Get current day name
    
    logger.info(f"Current time: {now_local}, Day: {current_day}")

    # Define shift timings and labels
    if time(6, 0) <= current_time < time(14, 0):
        shift_start = datetime.combine(today, time(6, 0))
        shift_end = datetime.combine(today, time(14, 0))
        shift_label = "Shift 1 (06:00 - 14:00 Local)"
    elif time(14, 0) <= current_time < time(22, 0):
        shift_start = datetime.combine(today, time(14, 0))
        shift_end = datetime.combine(today, time(22, 0))
        shift_label = "Shift 2 (14:00 - 22:00 Local)"
    else:
        if current_time >= time(22, 0):
            shift_start = datetime.combine(today, time(22, 0))
            shift_end = datetime.combine(today + timedelta(days=1), time(6, 0))
        else:
            shift_start = datetime.combine(today - timedelta(days=1), time(22, 0))
            shift_end = datetime.combine(today, time(6, 0))
        shift_label = "Shift 3 (22:00 - 06:00 Local)"

    shift_start_str = shift_start.strftime('%Y-%m-%d %H:%M:%S')
    shift_end_str = shift_end.strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Shift details: {shift_label}, Start: {shift_start_str}, End: {shift_end_str}")

    # Calculate net production time in seconds
    net_production_time_seconds = (now_local - shift_start).total_seconds()

    # Define cycle times for each machine (in seconds)
    cycle_times = {
        "Mikron": 5.26,
        "Machine 1": 15,
        "Machine 2": 15,
        "Machine 3": 15,
        "TATP Parameter 1": 15.3,
        "TATP Parameter 2": 15.3
    }

    # Function to fetch SQL Server hourly data using different approaches
    def fetch_sql_server_hourly_data(table_name, shift_start, shift_end, timestamp_column="TimeStamp", part_count_column="PartCount", method="auto"):
        logger.info(f"Fetching SQL Server data for table: {table_name}, method: {method}")
        
        # Create a default result with all hours in the shift and zero values
        expected_hours = pd.date_range(start=shift_start, end=shift_end - timedelta(minutes=1), freq='h')
        df_result = pd.DataFrame({'HOUR': expected_hours})
        df_result['LABEL_HOUR'] = df_result['HOUR'].dt.strftime('%H:00')
        df_result['PART_COUNT'] = 0
        df_result['RECORD_COUNT'] = 0
        
        try:
            # Format dates as strings for SQL Server
            start_str = shift_start.strftime('%Y-%m-%d %H:%M:%S')
            end_str = shift_end.strftime('%Y-%m-%d %H:%M:%S')
            
            # First, check if the table exists and get its columns
            check_query = f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}'
            """
            
            columns_df = pd.read_sql(check_query, sql_engine)
            
            # Check if the timestamp column exists
            if timestamp_column not in columns_df['COLUMN_NAME'].values:
                # Try common alternatives
                alternatives = ['timestamp', 'datetime', 'date', 'time', 'created_at', 'updated_at']
                for alt in alternatives:
                    if alt in columns_df['COLUMN_NAME'].values:
                        timestamp_column = alt
                        logger.info(f"Using alternative timestamp column: {alt}")
                        break
                else:
                    logger.warning(f"No suitable timestamp column found for table {table_name}")
                    return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]
            
            # Check if part count column exists
            if part_count_column not in columns_df['COLUMN_NAME'].values:
                # Try common alternatives
                alternatives = ['part_count', 'parts', 'quantity', 'count']
                for alt in alternatives:
                    if alt in columns_df['COLUMN_NAME'].values:
                        part_count_column = alt
                        logger.info(f"Using alternative part count column: {alt}")
                        break
                else:
                    logger.warning(f"No suitable part count column found for table {table_name}")
                    return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]
            
            # Get all raw data for the shift period
            raw_query = f"""
                SELECT {timestamp_column}, {part_count_column}
                FROM {table_name}
                WHERE {timestamp_column} >= '{start_str}' AND {timestamp_column} < '{end_str}'
                ORDER BY {timestamp_column}
            """
            
            df_raw = pd.read_sql(raw_query, sql_engine)
            logger.info(f"Retrieved {len(df_raw)} raw records from {table_name}")
            
            if df_raw.empty:
                logger.warning(f"No data found for table {table_name} in the specified time range")
                return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]
            
            # Convert timestamp to datetime
            df_raw[timestamp_column] = pd.to_datetime(df_raw[timestamp_column])
            
            # Create a column for the hour
            df_raw['HOUR'] = df_raw[timestamp_column].dt.floor('H')
            
            # Create a DataFrame with all hours in the shift
            expected_hours = pd.date_range(start=shift_start, end=shift_end - timedelta(minutes=1), freq='h')
            df_result = pd.DataFrame({'HOUR': expected_hours})
            
            # Calculate part count based on the selected method
            if method == "auto":
                # Try different methods and pick the one that gives non-zero results
                methods_to_try = ["minmax", "distinct", "changes", "nonzero"]
                best_method = None
                best_result = None
                best_nonzero_hours = 0
                
                for method_name in methods_to_try:
                    if method_name == "minmax":
                        # Min/max approach
                        hourly_data = df_raw.groupby('HOUR').agg({
                            part_count_column: ['min', 'max', 'count']
                        }).reset_index()
                        hourly_data.columns = ['HOUR', 'MIN_PART_COUNT', 'MAX_PART_COUNT', 'RECORD_COUNT']
                        hourly_data['PART_COUNT'] = hourly_data['MAX_PART_COUNT'] - hourly_data['MIN_PART_COUNT']
                        
                        # If negative, set to 0 and warn
                        mask = hourly_data['PART_COUNT'] < 0
                        if mask.any():
                            hourly_data.loc[mask, 'PART_COUNT'] = 0
                            logger.warning(f"Found negative part counts using minmax method, set to 0")
                        
                        method_result = hourly_data[['HOUR', 'PART_COUNT', 'RECORD_COUNT']]
                        
                    elif method_name == "distinct":
                        # Count distinct values approach
                        hourly_data = df_raw.groupby('HOUR').agg({
                            part_count_column: pd.Series.nunique
                        }).reset_index()
                        hourly_data.columns = ['HOUR', 'PART_COUNT']
                        
                        # Get record count separately
                        record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                        
                        method_result = pd.merge(hourly_data, record_counts, on='HOUR')
                        
                    elif method_name == "changes":
                        # Count changes in value approach
                        df_sorted = df_raw.sort_values(timestamp_column)
                        df_sorted['PREV_VALUE'] = df_sorted[part_count_column].shift(1)
                        df_sorted['CHANGED'] = (df_sorted[part_count_column] != df_sorted['PREV_VALUE']).astype(int)
                        
                        # First record always counts as a change
                        df_sorted.loc[df_sorted[timestamp_column] == df_sorted.groupby('HOUR')[timestamp_column].transform('min'), 'CHANGED'] = 1
                        
                        hourly_data = df_sorted.groupby('HOUR').agg({
                            'CHANGED': 'sum'
                        }).reset_index()
                        hourly_data.columns = ['HOUR', 'PART_COUNT']
                        
                        # Get record count separately
                        record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                        
                        method_result = pd.merge(hourly_data, record_counts, on='HOUR')
                        
                    elif method_name == "nonzero":
                        # Count non-zero values approach
                        nonzero_data = df_raw[df_raw[part_count_column] > 0]
                        hourly_data = nonzero_data.groupby('HOUR').size().reset_index(name='PART_COUNT')
                        
                        # Get record count separately
                        record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                        
                        method_result = pd.merge(hourly_data, record_counts, on='HOUR', how='right')
                        method_result['PART_COUNT'] = method_result['PART_COUNT'].fillna(0).astype(int)
                    
                    # Count non-zero hours
                    nonzero_hours = (method_result['PART_COUNT'] > 0).sum()
                    
                    if nonzero_hours > best_nonzero_hours:
                        best_nonzero_hours = nonzero_hours
                        best_method = method_name
                        best_result = method_result
                
                logger.info(f"Selected best method: {best_method} with {best_nonzero_hours} non-zero hours")
                df_result = pd.merge(df_result, best_result, on='HOUR', how='left')
                
            else:
                # Use the specified method
                if method == "minmax":
                    # Min/max approach
                    hourly_data = df_raw.groupby('HOUR').agg({
                        part_count_column: ['min', 'max', 'count']
                    }).reset_index()
                    hourly_data.columns = ['HOUR', 'MIN_PART_COUNT', 'MAX_PART_COUNT', 'RECORD_COUNT']
                    hourly_data['PART_COUNT'] = hourly_data['MAX_PART_COUNT'] - hourly_data['MIN_PART_COUNT']
                    
                    # If negative, set to 0 and warn
                    mask = hourly_data['PART_COUNT'] < 0
                    if mask.any():
                        hourly_data.loc[mask, 'PART_COUNT'] = 0
                        logger.warning(f"Found negative part counts using minmax method, set to 0")
                    
                    df_result = pd.merge(df_result, hourly_data[['HOUR', 'PART_COUNT', 'RECORD_COUNT']], on='HOUR', how='left')
                    
                elif method == "distinct":
                    # Count distinct values approach
                    hourly_data = df_raw.groupby('HOUR').agg({
                        part_count_column: pd.Series.nunique
                    }).reset_index()
                    hourly_data.columns = ['HOUR', 'PART_COUNT']
                    
                    # Get record count separately
                    record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                    
                    df_result = pd.merge(df_result, hourly_data, on='HOUR', how='left')
                    df_result = pd.merge(df_result, record_counts, on='HOUR', how='left')
                    
                elif method == "changes":
                    # Count changes in value approach
                    df_sorted = df_raw.sort_values(timestamp_column)
                    df_sorted['PREV_VALUE'] = df_sorted[part_count_column].shift(1)
                    df_sorted['CHANGED'] = (df_sorted[part_count_column] != df_sorted['PREV_VALUE']).astype(int)
                    
                    # First record always counts as a change
                    df_sorted.loc[df_sorted[timestamp_column] == df_sorted.groupby('HOUR')[timestamp_column].transform('min'), 'CHANGED'] = 1
                    
                    hourly_data = df_sorted.groupby('HOUR').agg({
                        'CHANGED': 'sum'
                    }).reset_index()
                    hourly_data.columns = ['HOUR', 'PART_COUNT']
                    
                    # Get record count separately
                    record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                    
                    df_result = pd.merge(df_result, hourly_data, on='HOUR', how='left')
                    df_result = pd.merge(df_result, record_counts, on='HOUR', how='left')
                    
                elif method == "nonzero":
                    # Count non-zero values approach
                    nonzero_data = df_raw[df_raw[part_count_column] > 0]
                    hourly_data = nonzero_data.groupby('HOUR').size().reset_index(name='PART_COUNT')
                    
                    # Get record count separately
                    record_counts = df_raw.groupby('HOUR').size().reset_index(name='RECORD_COUNT')
                    
                    df_result = pd.merge(df_result, hourly_data, on='HOUR', how='right')
                    df_result['PART_COUNT'] = df_result['PART_COUNT'].fillna(0).astype(int)
            
            # Fill missing values with 0
            df_result['PART_COUNT'] = df_result['PART_COUNT'].fillna(0).astype(int)
            df_result['RECORD_COUNT'] = df_result['RECORD_COUNT'].fillna(0).astype(int)
            df_result['LABEL_HOUR'] = df_result['HOUR'].dt.strftime('%H:00')
            
            return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]
            
        except Exception as e:
            logger.error(f"Error fetching SQL Server data for table {table_name}: {str(e)}")
            return df_result[['HOUR', 'LABEL_HOUR', 'PART_COUNT', 'RECORD_COUNT']]

    # Query Mikron data from Oracle
    logger.info("Querying Mikron data from Oracle")
    df_actual = pd.DataFrame()
    if oracle_engine:
        try:
            with oracle_engine.connect() as connection:
                connection.execute(text("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"))
                query = """
                    SELECT 
                        TO_CHAR(RESULT_DATE, 'YYYY-MM-DD HH24') AS RESULT_HOUR,
                        COUNT(*) AS RECORD_COUNT
                    FROM PDAECM.LOCATION_RESULTS
                    WHERE LOCATION_ID LIKE :loc_id
                      AND RESULT_DATE >= TO_DATE(:start_time, 'YYYY-MM-DD HH24:MI:SS')
                      AND RESULT_DATE < TO_DATE(:end_time, 'YYYY-MM-DD HH24:MI:SS')
                    GROUP BY TO_CHAR(RESULT_DATE, 'YYYY-MM-DD HH24')
                    ORDER BY RESULT_HOUR
                """
                df_actual = pd.read_sql(
                    query,
                    con=connection,
                    params={
                        "loc_id": "%829%",
                        "start_time": shift_start_str,
                        "end_time": shift_end_str,
                    }
                )
                df_actual.columns = [col.upper() for col in df_actual.columns]
                logger.info(f"Retrieved Mikron data: {len(df_actual)} records")
        except Exception as e:
            logger.error(f"Error querying Mikron data: {str(e)}")

    # Prepare Mikron dataframe
    expected_hours = pd.date_range(start=shift_start, end=shift_end - timedelta(minutes=1), freq='h')
    df_expected = pd.DataFrame({'RESULT_HOUR': expected_hours})

    if not df_actual.empty:
        df_actual['RESULT_HOUR'] = pd.to_datetime(df_actual['RESULT_HOUR'], format='%Y-%m-%d %H')
        df_merged = pd.merge(df_expected, df_actual, on='RESULT_HOUR', how='left')
        df_merged['RECORD_COUNT'] = df_merged['RECORD_COUNT'].fillna(0).astype(int)
    else:
        df_merged = df_expected.copy()
        df_merged['RECORD_COUNT'] = 0

    df_merged['LABEL_HOUR'] = df_merged['RESULT_HOUR'].dt.strftime('%H:00')

    # Create Mikron chart
    fig_mikron = px.bar(
        df_merged,
        x='LABEL_HOUR',
        y='RECORD_COUNT',
        labels={"LABEL_HOUR": "Hour", "RECORD ) 