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
import time

import numpy as np

# Use Airflow's logger
logging = LoggingMixin().log

# Fetch environment variables with error handling
# DB_SERVER = os.getenv('DB_SERVER', 'host.docker.internal')
# DB_USER = os.getenv('DB_USER', 'sa')
# DB_PASSWORD = os.getenv('DB_PASSWORD', 'YourStrong!Passw0rd')
# DB_NAME = os.getenv('DB_NAME', 'ighd')

# DB_SERVER = os.getenv('DB_SERVER', 'cls-pae-fp51764')
DB_SERVER = os.getenv('DB_SERVER', '10.1.182.132')
DB_USER = os.getenv('DB_USER', 'app')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'abcd1234')
DB_NAME = os.getenv('DB_NAME', 'ighd_devices')

if not all([DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME]):
    logging.error("Missing environment variables for database connection!")
    raise EnvironmentError("One or more database environment variables are missing.")

SLACK_CONN_ID = 'slack_conn'
hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)

FOLDER_PATH = '/opt/airflow/dags/csv/tempu_data'
DONE_FOLDER = os.path.join(FOLDER_PATH, 'done')


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


def move_file_to_done(file_name):
    done_folder = os.path.join(FOLDER_PATH, 'done')
    file_path = os.path.join(FOLDER_PATH, file_name)

    logging.info(f"Moving file {file_name} from {file_path} to {done_folder} folder.")

    try:
        os.makedirs(done_folder, exist_ok=True)  # Ensure the "done" folder exists
        shutil.move(file_path, os.path.join(done_folder, os.path.basename(file_name)))
        message = f":white_check_mark: File `{file_name}` has been processed and moved to 'done'."
        hook.send(text=message)
    except Exception as e:
        logging.error(f"Error moving file {file_name}: {str(e)}")
        hook.send(text=f":triangular_flag_on_post: Error moving file {file_name}: {str(e)}")


def get_sql_connection(retries=5, delay=5):
    logging.info("    > Getting SQL Connection.")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        # "Encrypt=yes;"
        "TrustServerCertificate=yes;"  # Required if using self-signed cert in Docker
        "LoginTimeout=10;"
    )

    for attempt in range(1, retries + 1):
        try:
            conn = pyodbc.connect(conn_str)
            pyodbc.pooling = True
            logging.info("    > SQL connection established.")
            return conn
        except pyodbc.Error as e:
            logging.error(f"[Attempt {attempt}/{retries}] Error while connecting to SQL Server: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.error("    > All connection attempts failed.")
                return None


def sanitize_filename(filename):
    filename = filename.replace(" ", "_")
    filename = re.sub(r'[^a-zA-Z0-9_-]', '', filename)
    if not filename.lower().endswith(".csv"):
        filename += ".csv"
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


def extract(**kwargs):
    conn = get_sql_connection()

    # #slack_alert(kwargs, status="started")
    file_pattern = os.path.join(FOLDER_PATH, '*.csv')

    files = glob.glob(file_pattern)

    if not files:
        logging.error("No CSV files found for extraction.")
        # slack_alert(kwargs, status="failed")
        raise DataExtractionError("No files found for extraction.")

    extracted_data = []
    for file in files:
        print(f"Processing: {file}")

        with open(file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Extract metadata using fixed keywords
        def get_value(keyword):
            for line in lines:
                if line.strip().startswith(keyword):
                    return line.split(":", 1)[1].strip()
            return None

        # Extract note on time format
        def get_time_format_note():
            for line in lines:
                if "All times shown are based on" in line:
                    return line.strip()
            return None

        metadata = {
            'DeviceID': get_value('ID').rstrip(','),
            'LogInterval': get_value('Log Interval').replace('min ', '').rstrip(',') if get_value(
                'Log Interval') else None,
            'StartMode': get_value('Start Mode').rstrip(','),
            'StopMode': get_value('Stop Mode').rstrip(','),
            'TripDuration': get_value('Trip Length').rstrip(','),
            'PointsLogged': get_value('Number of Points').rstrip(','),
            'TimeFormatNote': get_time_format_note()

        }

        # Find start of data
        header_index = next((idx for idx, line in enumerate(lines) if line.strip().startswith('Date')), None)
        if header_index is None:
            raise ValueError(f"Header row not found in file: {file}")

        # Read data from CSV
        df = pd.read_csv(file, skiprows=header_index)
        df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
        df = df.iloc[1:].reset_index(drop=True)
        # Add metadata to all rows
        df['DeviceID'] = metadata.get('DeviceID')
        df['LogInterval'] = metadata.get('LogInterval')
        df['StartMode'] = metadata.get('StartMode')
        df['StopMode'] = metadata.get('StopMode')
        df['TripDuration'] = metadata.get('TripDuration')
        df['PointsLogged'] = metadata.get('PointsLogged')
        df['TimeFormatNote'] = metadata.get('TimeFormatNote')
        df['File Name'] = os.path.basename(file)

        # print(f"Columns: {df.columns.tolist()}")
        df = clean_df(df)
        logging.info(f"Extracted {df.shape[0]} rows from {file}")
        extracted_data.append({'table': df.to_json(orient='records')})

    # Push to XCom
    kwargs['ti'].xcom_push(key='extracted_data', value=json.dumps(extracted_data))


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


def transform(**kwargs):
    # slack_alert(kwargs, status="started")

    ti = kwargs['ti']
    extracted_data_json = ti.xcom_pull(key='extracted_data', task_ids='extract_task')
    extracted_data = json.loads(extracted_data_json) if extracted_data_json else []
    transformed_data = []

    if not extracted_data:
        logging.error("No extracted data found in XCom.")
        # slack_alert(kwargs, status="failed")
        raise DataTransformationError("No data to transform")

    # Define float columns based on your table schema
    float_columns = [
        'WetBulbTemp', 'GlobeTemperature', 'RelativeHumidity',
        'BarometricPressure', 'StationPressure', 'WindSpeed', 'HeatIndex',
        'DewPoint', 'DensityAltitude', 'NWBTemp', 'ThermalWorkLimit',
        'WetBulbGlobeTemperature', 'WindChill'
    ]

    try:
        for item in extracted_data:
            df = pd.read_json(io.StringIO(item['table']))

            # Rename columns if not already done earlier
            column_map = {
                'FORMATTED DATE_TIME': 'DateTime',
                'Wet Bulb Temp': 'WetBulbTemp',
                'Globe Temperature': 'GlobeTemperature',
                'Relative Humidity': 'RelativeHumidity',
                'Barometric Pressure': 'BarometricPressure',
                'Station Pressure': 'StationPressure',
                'Wind Speed': 'WindSpeed',
                'Heat Index': 'HeatIndex',
                'Dew Point': 'DewPoint',
                'Density Altitude': 'DensityAltitude',
                'Compass Magnetic Direction': 'CompassMagneticDirection',
                'NWB Temp': 'NWBTemp',
                'Compass True Direction': 'CompassTrueDirection',
                'Thermal Work Limit': 'ThermalWorkLimit',
                'Wet Bulb Globe Temperature': 'WetBulbGlobeTemperature',
                'Wind Chill': 'WindChill',
                'Data Type': 'DataType',
                'Record name': 'RecordName',
                'Start time': 'StartTime',
                'Duration (H:M:S)': 'Duration',
                'Location description': 'LocationDescription',
                'Location address': 'LocationAddress',
                'Location coordinates': 'LocationCoordinates'
            }
            df.rename(columns=column_map, inplace=True)

            # Drop first row (after header)
            df.drop(index=0, inplace=True)

            # Parse datetime
            # df = fix_datetime_format(df)

            # Convert float columns safely
            for col in float_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Handle missing values (from the separate function)
            # df = handle_missing_values(df)

            # Append clean records
            transformed_data.extend(json.loads(df.to_json(orient='records')))

    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        # slack_alert(kwargs, status="failed")
        raise DataTransformationError("Data transformation failed") from e

    ti.xcom_push(key='transformed_data', value=json.dumps(transformed_data))


def load(**kwargs):
    # slack_alert(kwargs, status="started")

    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_task')
    if not extracted_data:
        raise ValueError("No data found in XCom for loading.")

    logging.info("Loading data into SQL Server...")
    extracted_data = json.loads(extracted_data)
    conn = get_sql_connection()
    cursor = conn.cursor()
    filenames = []
    filename = ""
    rowcount = 0
    fileCount = 0

    logging.info("Processing data entry...")
    for data_entry in extracted_data:
        if rowcount != 0:
            logging.info(f"Inserted {rowcount} rows for filename: {filename}")
        # df = pd.read_json(data_entry['table'], orient='records')
        df = pd.read_json(io.StringIO(data_entry['table']), orient='records')

        # Fix and clean data
        # df = fix_datetime_format(df)
        # df = handle_missing_values(df)

        # Get filename for this dataframe
        # filename = df['filename'].iloc[0] if 'filename' in df.columns else 'unknown.csv'
        # print(df.dtypes)
        # print(df['DeviceID'].unique())

        # Prepare insert query
        for _, row in df.iterrows():
            if filename == row['filename']:
                if rowcount == 0:
                    logging.info(f"Processing file: {filename}")
            else:
                # logging.info(f"Inserted {rowcount} rows for filename: {filename}")
                move_file_to_done(filename)
                fileCount += 1
                filename = row['filename']
                logging.info(f"Processing file: {filename}")
                rowcount = 0
            try:
                # logging.info(f"Inserting row from file {row['filename']} into database...")
                insert_query = """
                               INSERT INTO tempu_data (LogDateTime, TemperatureC, HumidityRH, DeviceID, LogInterval,
                                                       StartMode, StopMode, TripDuration, PointsLogged, TimeFormatNote,
                                                       filename)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                               """

                insert_data = tuple(row.get(col, None) for col in [
                    'LogDateTime',
                    'Temperature(C)',  # Will map to TemperatureC
                    'Humidity(%RH)',  # Will map to HumidityRH
                    'DeviceID',
                    'LogInterval',
                    'StartMode',
                    'StopMode',
                    'TripDuration',
                    'PointsLogged',
                    'TimeFormatNote',
                    'filename'
                ])

                # for i, val in enumerate(insert_data):
                #     logging.info(f"Param {i + 1}: {val} (type: {type(val)})")
                # print(f"Inserting data: {insert_data}")
                cursor.execute(insert_query, insert_data)
                rowcount += 1
            except pyodbc.IntegrityError as e:
                if "UQ_LogDateTime_DeviceID" in str(e):
                    logging.warning(
                        f"Duplicate entry skipped (LogDateTime: {row['LogDateTime']}, DeviceID: {row['DeviceID']}) in file: {row['filename']}")
                    continue  # skip and move on to next row
                else:
                    logging.error(f"Integrity error in file {row['filename']}: {e}")
                    conn.rollback()
                    raise
            except Exception as e:
                logging.info(
                    f"Error inserting row: Temperature(C): {row['Temperature(C)']} Humidity(%RH): {row['Humidity(%RH)']} DeviceID: {row['DeviceID']} LogInterval: {row['LogInterval']} StartMode: {row['StartMode']} StopMode: {row['StopMode']} TripDuration: {row['TripDuration']} PointsLogged: {row['PointsLogged']} TimeFormatNote: {row['TimeFormatNote']}")
                logging.error(f"Error inserting row from file {row['filename']}: {e}")
                conn.rollback()
                raise

        conn.commit()
        # move_file_to_done(file_name)
    move_file_to_done(filename)
    logging.info(f"Data loaded successfully. Processed '{fileCount}' files")
    cursor.close()
    delete_duplicates()
    conn.close()
    # slack_alert(kwargs, status="success")


# Define the DAG
dag = DAG(
    'tempu_data_etl_dag',
    description='Pipeline to process Tempu weather data and insert into the database',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 6),
    catchup=False,
)

# Define the tasks
wait_for_file_task = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_conn_tempu",
    filepath="*.csv",
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
