import io
import os
import shutil
import glob
import logging
import json
import re
import pyodbc
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from dateutil import parser
import pytz
from datetime import timedelta

import numpy as np

# Use Airflow's logger
logging = LoggingMixin().log

# Development Database
# DB_SERVER = os.getenv('DB_SERVER', 'host.docker.internal')
# DB_USER = os.getenv('DB_USER', 'sa')
# DB_PASSWORD = os.getenv('DB_PASSWORD', 'YourStrong!Passw0rd')
# DB_NAME = os.getenv('DB_NAME', 'ighd')

# Production Database
DB_SERVER = os.getenv('DB_SERVER', '10.1.1.244')
DB_USER = os.getenv('DB_USER', 'ighdapp')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'R8m!zK2@qL')
DB_NAME = os.getenv('DB_NAME', 'ighd')

if not all([DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME]):
    logging.error("Missing environment variables for database connection!")
    raise EnvironmentError("One or more database environment variables are missing.")

SLACK_CONN_ID = 'slack_conn'
hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)

FOLDER_PATH = '/opt/airflow/dags/csv/ibutton_data'
DONE_FOLDER = os.path.join(FOLDER_PATH, 'done')
TEMP_FOLDER = os.path.join(FOLDER_PATH, 'temp')


class DataExtractionError(Exception):
    pass


# Slack Alert Function
def slack_alert(context, status="failed"):
    task_instance = context.get('task_instance')
    airflow_base_url = "http://cls-pae-fl71541:8080/"
    log_url = task_instance.log_url.replace("http://localhost:8080/", airflow_base_url)

    if status == "success":
        emoji = ":white_check_mark:"
        status_text = "Task Succeeded"
    elif status == "started":
        emoji = ":arrow_forward:"
        status_text = "Task Started"
    else:
        emoji = ":red_circle:"
        status_text = "Task Failed"

    slack_msg = f"""
    {emoji} *{status_text}*
    *Task:* `{task_instance.task_id}`
    *DAG:* `{task_instance.dag_id}`
    *Execution Time:* `{context.get('execution_date')}`
    *Log URL:* <{log_url}|View Logs>
    """

    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)  # replace with your conn ID
    hook.send(text=slack_msg)


def move_file_to_done(file_name, done_folder_name="done"):
    done_folder = os.path.join(FOLDER_PATH, done_folder_name)
    file_path = os.path.join(FOLDER_PATH, file_name)
    temp_file = os.path.join(FOLDER_PATH, TEMP_FOLDER, os.path.basename(file_name))

    logging.info(f"\t -- Moving file...")

    try:
        os.makedirs(done_folder, exist_ok=True)  # Ensure the "done" folder exists
        shutil.move(file_path, os.path.join(done_folder, os.path.basename(file_name)))

        # ✅ Delete temp file after successful move
        if os.path.exists(temp_file):
            os.remove(temp_file)
            logging.info(f"\t -- Temporary file deleted successfully.")
        else:
            logging.warning(f"*** -> Temporary file not found for deletion.")

        message = f":white_check_mark: File `{file_name}` has been processed and moved to 'done'."
        hook.send(text=message)
    except Exception as e:
        logging.error(f"*** --> Error moving file: {str(e)}")
        hook.send(text=f":triangular_flag_on_post: Error moving file {file_name}: {str(e)}")


def get_sql_connection():
    logging.info("    > Getting SQL Connection.")
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={DB_SERVER};'
        f'DATABASE={DB_NAME};'
        f'UID={DB_USER};'
        f'PWD={DB_PASSWORD};'
        'Encrypt=no;'  # Disable encryption since it's not a secure server
    )
    try:
        conn = pyodbc.connect(conn_str)
        pyodbc.pooling = True
        return conn
    except pyodbc.Error as e:
        logging.error(f"Error while connecting to the SQL Server: {e}")
        return None


def sanitize_filename(filename):
    filename = filename.replace(" ", "_")
    filename = re.sub(r'[^a-zA-Z0-9_-]', '', filename)
    if not filename.lower().endswith(".xlsx"):
        filename += ".xlsx"
    return filename


# def clean_df(df):
#     # print(f"Columns before cleaning: {df.columns.tolist()}")
#
#     # Rename columns to match DB schema
#
#     column_map = {
#         'Date': 'LogDate',  # Timestamp of the log
#         'Time': 'LogTime',  # Timestamp of the log
#         'Temperature (°C)': 'TemperatureC',  # Temperature in Celsius
#         'Humidity (%)': 'HumidityRH',  # Relative humidity
#         'Device ID': 'DeviceID',  # Device identifier
#         'Log Interval (s)': 'LogInterval',  # Time between readings
#         'Start Mode': 'StartMode',  # Logging start mode
#         'Stop Mode': 'StopMode',  # Logging stop mode
#         'Trip Duration': 'TripDuration',  # Duration of the logging trip
#         'Points Logged': 'PointsLogged',  # Number of logged data points
#         'Serial Number': 'DeviceID',  # Redundant with Device ID? Keep if needed
#         'File Name': 'filename',  # For tracking origin
#     }
#
#     df.rename(columns=column_map, inplace=True)
#
#     # Combine Date and Time into a single datetime column
#     # # # IMPORTANT! 17-02-25, 15:08:10 => 2025-02-17 15:08:10
#     # Combine LogDate and LogTime safely
#     df['LogDateTimeStr'] = df['LogDate'].astype(str).str.strip() + ' ' + df['LogTime'].astype(str).str.strip()
#
#     # Parse datetime with auto-format detection
#     def safe_parse_datetime(dt_str):
#         try:
#             return parser.parse(dt_str, dayfirst=True)  # Adjust dayfirst based on your region
#         except Exception:
#             return pd.NaT
#
#     df['LogDateTime'] = df['LogDateTimeStr'].apply(safe_parse_datetime)
#
#     # Optional: convert to standardized string format
#     df['LogDateTime'] = df['LogDateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
#
#     df.drop(columns=['LogDate', 'LogTime'], inplace=True)
#
#     return df


def clean_df(df):
    column_map = {
        'Date': 'LogDate',
        'Time': 'LogTime',
        'Temperature (°C)': 'TemperatureC',
        'Temperature(C)': 'TemperatureC',
        'Humidity(%RH)': 'HumidityRH',
        'Humidity (%)': 'HumidityRH',
        'Device ID': 'DeviceID',
        'Log Interval (s)': 'LogInterval',
        'Start Mode': 'StartMode',
        'Stop Mode': 'StopMode',
        'Trip Duration': 'TripDuration',
        'Points Logged': 'PointsLogged',
        'Serial Number': 'DeviceID',  # Optional
        'File Name': 'filename',
    }
    df.rename(columns=column_map, inplace=True)

    df['LogDateTimeStr'] = df['LogDate'].astype(str).str.strip() + ' ' + df['LogTime'].astype(str).str.strip()

    def parse_time_format_note(note):
        dt_format = "%d/%m/%y %H:%M:%S"  # default
        if note and isinstance(note, str):
            fmt_match = re.search(r'\[(.*?)\]', note)
            # logging.info(f"Time format: {fmt_match.group(1)}")
            if fmt_match:
                fmt_raw = fmt_match.group(1).upper().replace(':MM', ':mm')  # Handle MM as minutes
                fmt = fmt_raw.replace('YYYY', '%Y').replace('DD', '%d').replace('MM', '%m').replace('YY', '%Y').replace(
                    'HH', '%H').replace('hh', '%I').replace(':mm', ':%M').replace('SS', '%S')
                dt_format = fmt
                # logging.info(f"Final datetime format used: {dt_format}")

        return dt_format

    time_format_note = df['TimeFormatNote'].iloc[0] if 'TimeFormatNote' in df.columns else None
    dt_format = parse_time_format_note(time_format_note) or "%d/%m/%Y %H:%M:%S"

    # logging.info(f"Using datetime format: {dt_format}")

    def safe_parse_datetime(dt_str):
        try:
            dt = pd.to_datetime(dt_str, format=dt_format, errors='raise')
            return dt
        except Exception as e:
            logging.warning(f"Failed to parse '{dt_str}' with format '{dt_format}': {e}")
            message = f"Failed to parse '{dt_str}' with format '{dt_format}': {e}"
            hook.send(text=message)
            raise
            # return pd.NaT

    # logging.info(f"Parsed LogDateTimeStr: {df['LogDateTimeStr'].head()}")
    df['LogDateTime'] = df['LogDateTimeStr'].apply(safe_parse_datetime)
    # logging.info(f"Parsed LogDateTime: {df['LogDateTime'].head()}")
    df['LogDateTime'] = df['LogDateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # logging.info(f"Formatted LogDateTime: {df['LogDateTime'].head()}")

    df.drop(columns=['LogDate', 'LogTime'], inplace=True)

    return df


class DataTransformationError(Exception):
    pass


# Function to fix time format and convert date/time
def fix_datetime_format(df):
    # Convert 'DateTime' column to datetime with error handling
    df['DateTime'] = pd.to_datetime(df['DateTime'], format='%Y-%m-%d %I:%M:%S %p', errors='coerce')

    # Replace '.' with ':' in the time string if necessary
    df['DateTime'] = df['DateTime'].apply(lambda x: str(x).replace('.', ':') if isinstance(x, str) else x)

    # Re-format DateTime to '%Y-%m-%d %H:%M:%S'
    df['DateTime'] = df['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df


def handle_missing_values(df):
    # Step 1: Define invalid markers to standardize as missing
    invalid_markers = ['--', 'NULL', '']
    categorical_columns = [
        'LocationDescription', 'DataType', 'filename', 'Crosswind', 'Headwind',
        'CompassMagneticDirection', 'CompassTrueDirection', 'Notes'
    ]

    numerical_columns = [
        'Temperature', 'WetBulbTemp', 'GlobeTemperature', 'RelativeHumidity',
        'BarometricPressure', 'Altitude', 'StationPressure', 'WindSpeed',
        'HeatIndex', 'DewPoint', 'DensityAltitude', 'NWBTemp', 'ThermalWorkLimit',
        'WetBulbGlobeTemperature', 'WindChill'
    ]

    # Step 2: Replace common invalid markers with actual NaN
    df.replace(invalid_markers, np.nan, inplace=True)

    # Step 3: Categorical columns → NaN to None
    for col in categorical_columns:
        if col in df.columns:
            df[col] = df[col].where(pd.notna(df[col]), None)

    # Step 4: Numerical columns → preserve NaN to become SQL NULL (via None)
    for col in numerical_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Ensure numeric type
            df[col] = df[col].where(pd.notna(df[col]), None)  # Replace NaN with None
            missing_count = df[col].isnull().sum()
            if missing_count:
                logging.info(f"Column '{col}' has {missing_count} missing values replaced with NULL (None).")

    # Step 5: Catch-all for any remaining NaN values in the DataFrame
    df = df.applymap(lambda x: None if pd.isna(x) else x)

    # Log overall data shape and preview
    logging.info(f"Transformed data contains {df.shape[0]} rows and {df.shape[1]} columns.")
    logging.info(f"Transformed data preview columns: {df.columns.tolist()}")

    # Step 6: Group and log missing data by 'filename'
    if 'filename' in df.columns:
        missing_data_grouped = df.isnull().groupby(df['filename']).sum()
        missing_data_grouped = missing_data_grouped.loc[:, (missing_data_grouped != 0).any(axis=0)]

        for filename, row in missing_data_grouped.iterrows():
            missing_cols = row[row > 0]
            if not missing_cols.empty:
                logging.warning(f"Missing data for filename '{filename}': \n{missing_cols}")
                message = f"Missing data for filename '{filename}': \n{missing_cols}"
                hook.send(text=message)

    return df  # Return the updated dataframe after handling missing values


def delete_duplicates():
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        print("Executing stored procedure to delete duplicates...")

        # Execute the stored procedure
        cursor.execute("{CALL Delete_Duplicates_LogData_IButtonData}")

        # Fetch and display PRINT output messages from SQL Server (if any)
        while True:
            message = cursor.get_messages()
            if not message:
                hook.send_text("No messages returned from SQL Server.")
                break
            for msg in message:
                logging(f"SQL_Log: {msg[1]}")  # msg[1] is the message text
                message = f"SQL_Log: {msg[1]}"
                hook.send_text(message)

        logging("Duplicates removed successfully.")
        message = "Duplicates removed successfully."
        hook.send_text(message)

    except Exception as e:
        print("Error:", e)


def extract(**kwargs):
    files = glob.glob(os.path.join(FOLDER_PATH, '*.xlsx'))
    if not files:
        raise DataExtractionError("No files found for extraction.")

    logging.info(f"Found {len(files)} files for extraction.")

    # Just push file paths
    kwargs['ti'].xcom_push(key='file_list', value=files)


def transform(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(key='file_list', task_ids='extract_task')

    if not files:
        raise DataTransformationError("No files to transform.")

    processed_files = []

    for file in files:
        logging.info(f"Transforming: {file}")

        try:
            # ✅ STEP 1: Read raw (no header)
            raw_df = pd.read_excel(file, header=None, engine='openpyxl')

            # ✅ STEP 2: Find where actual data starts
            header_row = None
            for i, row in raw_df.iterrows():
                if str(row[0]).strip() == 'Date' and str(row[1]).strip() == 'Time':
                    header_row = i
                    break

            if header_row is None:
                raise ValueError(f"Header row not found in {file}")

            logging.info(f"Header found at row: {header_row}")

            # ✅ STEP 3: Read again with correct header
            df = pd.read_excel(file, skiprows=header_row, engine='openpyxl')

            df = df.drop(df.index[-1]) if not df.empty else df

            # ✅ Clean column names (VERY IMPORTANT)
            df.columns = [str(col).strip() for col in df.columns]

            # Remove unwanted extra columns
            df = df[['Date', 'Time', 'Value']]

            # ✅ STEP 4: Datetime creation
            if 'Date' in df.columns and 'Time' in df.columns:
                df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str),
                                                format='%Y-%m-%d %H:%M:%S', errors='coerce').dt.strftime(
                    '%Y-%m-%d %H:%M:%S')

            # 🚨 CRITICAL
            before = len(df)
            # df = df.dropna(subset=['Datetime'])
            after = len(df)

            logging.info(f"Dropped {before - after} bad datetime rows")

            # df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # ✅ Add required fields for DB
            df['Data_Unit'] = 'degrees C'
            df['Mission_Sample_Count'] = None  # optional (or parse from metadata)
            df['deviceID'] = os.path.basename(file).split('_')[2]  # adjust if needed
            df['filename'] = os.path.basename(file)

            # Keep only DB columns
            df = df[['Datetime', 'Value', 'Data_Unit', 'Mission_Sample_Count', 'deviceID', 'filename']]

            if df.empty:
                logging.warning(f"No valid data: {file}")
                move_file_to_done(os.path.basename(file), "NoData")
                continue

            # ✅ Save CSV
            temp_file = os.path.join(
                FOLDER_PATH,
                "temp",
                os.path.basename(file).replace(".xlsx", ".csv")
            )

            os.makedirs(os.path.dirname(temp_file), exist_ok=True)
            df.to_csv(temp_file, index=False)

            processed_files.append(temp_file)

        except Exception as e:
            logging.error(f"Error processing {file}: {e}")
            hook.send(text=f"❌ Transform error: {file} → {str(e)}")
            move_file_to_done(os.path.basename(file), "failed")
            continue

    ti.xcom_push(key='processed_files', value=processed_files)


def load(**kwargs):
    ti = kwargs['ti']
    processed_files = ti.xcom_pull(key='processed_files', task_ids='transform_task')

    if not processed_files:
        raise ValueError("No processed files for loading.")

    conn = get_sql_connection()
    cursor = conn.cursor()

    for file in processed_files:
        logging.info(f"PROCESSING FILE: {file} ***")

        df = pd.read_csv(file)
        total_rows = len(df)
        insert_query = """
                       INSERT INTO ibutton_data (Datetime, Value, Data_Unit, Mission_Sample_Count, deviceID, filename)
                       VALUES (?, ?, ?, ?, ?, ?) \
                       """

        data_tuples = [
            tuple(row.get(col, None) for col in [

                'Datetime', 'Value', 'Data_Unit', 'Mission_Sample_Count',
                'deviceID',
                'filename'
            ])
            for _, row in df.iterrows()
        ]

        cursor.fast_executemany = True
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        logging.info(f"Inserted {len(data_tuples)} rows.")
        move_file_to_done(os.path.basename(file))

    cursor.close()
    conn.close()
    delete_duplicates()


# Define the DAG
dag = DAG(
    'ibutton_data_etl_dag_fast_tempu_copy',
    description='Pipeline to process Ibutton weather data and insert into the database',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 6),
    catchup=False,
)

# Define the tasks
wait_for_file_task = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_conn_ibutton",
    filepath="*.xlsx",
    poke_interval=5,
    timeout=600,
    mode="poke",

    # /opt/airflow/dags/csv/tempu_data
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    provide_context=True,
    dag=dag,
    on_execute_callback=lambda context: slack_alert(context, status="started"),
    on_success_callback=lambda context: slack_alert(context, status="success"),
    on_failure_callback=lambda context: slack_alert(context, status="failed"),
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
    on_execute_callback=lambda context: slack_alert(context, status="started"),
    on_success_callback=lambda context: slack_alert(context, status="success"),
    on_failure_callback=lambda context: slack_alert(context, status="failed")
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag,
    on_execute_callback=lambda context: slack_alert(context, status="started"),
    on_success_callback=lambda context: slack_alert(context, status="success"),
    on_failure_callback=lambda context: slack_alert(context, status="failed")
)

# Set task dependencies
wait_for_file_task >> extract_task >> transform_task >> load_task
