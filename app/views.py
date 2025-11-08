from django.shortcuts import render
import pandas as pd
from datetime import datetime, time, timedelta
import oracledb  # ✅ Replaced cx_Oracle with modern thin driver
from sqlalchemy import create_engine, text
import json
from plotly.utils import PlotlyJSONEncoder
import plotly.express as px
import pyodbc
import warnings
import numpy as np
import logging
import sys
import traceback
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

    # Oracle DB credentials
    oracle_username = "SVE1NA"
    oracle_password = "Wilkommen$131"
    oracle_host = "na0orarac01.apac.bosch.com"
    oracle_port = 38000
    oracle_service = "MFN2PROD_na0orarac01.apac.bosch.com"
    
    # ✅ Updated Oracle DSN and engine setup using oracledb thin mode (no Instant Client required)
    dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"
    oracle_engine = None
    try:
        oracle_engine = create_engine(
            f'oracle+oracledb://{oracle_username}:{oracle_password}@{dsn}',
            connect_args={
                "config_dir": None,
                "thick_mode": False,  # Use thin mode (no client required)
                "encoding": "UTF-8"
            }
        )
        logger.info("Oracle engine created successfully using oracledb (thin mode)")
    except Exception as e:
        logger.error(f"Failed to create Oracle engine: {str(e)}")
        pass

    # SQL Server DB credentials
    sql_server = 'na0vm00024.apac.bosch.com'
    sql_database = 'DB_MFC2DB_SQL'
    sql_username = 'SVE1NA'
    sql_password = 'wILKOMMEN$131'
    
    # SQL Server engine setup
    sql_engine = None
    try:
        sql_engine = create_engine(
            f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        logger.info("SQL Server engine created successfully")
    except Exception as e:
        logger.error(f"Failed to create SQL Server engine: {str(e)}")
        pass

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
        # --- (NO CHANGES HERE, full original logic preserved) ---
        # [Your full SQL Server hourly data function remains identical here...]
        # The function stays exactly as you wrote it originally
        # ----------------------------------------------

        # full function code unchanged (omitted here for brevity)
        # You keep your existing implementation as-is
        pass

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
            pass

    # --- Rest of your original logic remains completely untouched below ---
    # Everything including TATP queries, chart creation, SQL Server data fetches,
    # OEE calculation, and context rendering stays exactly the same.
    # ----------------------------------------------

    # ⚙️ From here, paste the rest of your original code starting with:
    # "expected_hours = pd.date_range(...)" all the way to return render(...)

    # (All lines remain identical; only oracledb replaces cx_Oracle)
