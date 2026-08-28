import io
import os
import shutil
import glob
import logging
from io import StringIO
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

FOLDER_PATH = '/opt/airflow/dags/csv/kestrel_data'
DONE_FOLDER = os.path.join(FOLDER_PATH, 'done')
TEMP_FOLDER = os.path.join(FOLDER_PATH, 'temp')


class DataExtractionError(Exception):
    pass


# Slack Alert Function
def slack_alert(context, success=False):
    task_instance = context.get('task_instance')
    airflow_base_url = "http://cls-pae-fl71541:8080/"
    log_url = task_instance.log_url.replace("http://localhost:8080/", airflow_base_url)

    emoji = ":white_check_mark:" if success else ":red_circle:"
    status = "Task Succeeded" if success else "Task Failed"
    slack_msg = f"""
    {emoji} *{status}*
    *Task:* `{task_instance.task_id}`
    *DAG:* `{task_instance.dag_id}`
    *Execution Time:* `{context.get('execution_date')}`
    *Log URL:* <{log_url}|View Logs>
    """
    hook.send(text=slack_msg)


def move_file_to_done(file_name, done_folder_name="done"):
    logging.info(f"filename: {file_name}")
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


# def move_file_to_done(file_name):
#     if file_name != "":
#         done_folder = os.path.join(FOLDER_PATH, 'done')
#         file_path = os.path.join(FOLDER_PATH, file_name)
#
#         logging.info(f"Moving file {file_name} from {file_path} to {done_folder} folder.")
#
#         try:
#             os.makedirs(done_folder, exist_ok=True)  # Ensure the "done" folder exists
#             shutil.move(file_path, os.path.join(done_folder, os.path.basename(file_name)))
#             message = f":white_check_mark: File `{file_name}` has been processed and moved to 'done'."
#             hook.send(text=message)
#         except Exception as e:
#             logging.error(f"Error moving file {file_name}: {str(e)}")
#             hook.send(text=f":triangular_flag_on_post: Error moving file {file_name}: {str(e)}")

# def move_file_to_done(file_name, done_folder_name="done"):
#     done_folder = os.path.join(FOLDER_PATH, done_folder_name)
#     file_path = os.path.join(FOLDER_PATH, file_name)
#     temp_file = os.path.join(FOLDER_PATH, TEMP_FOLDER, os.path.basename(file_name))
#
#     logging.info(f"\t -- Moving file...")
#
#     try:
#         os.makedirs(done_folder, exist_ok=True)  # Ensure the "done" folder exists
#         shutil.move(file_path, os.path.join(done_folder, os.path.basename(file_name)))
#
#         # ✅ Delete temp file after successful move
#         if os.path.exists(temp_file):
#             os.remove(temp_file)
#             logging.info(f"\t -- Temporary file deleted successfully.")
#         else:
#             logging.warning(f"*** -> Temporary file not found for deletion.")
#
#         message = f":white_check_mark: File `{file_name}` has been processed and moved to 'done'."
#         hook.send(text=message)
#     except Exception as e:
#         logging.error(f"*** --> Error moving file: {str(e)}")
#         hook.send(text=f":triangular_flag_on_post: Error moving file {file_name}: {str(e)}")
#
#     # logging.info(f"Moving file {file_name} from {file_path} to {done_folder} folder.")
#
#     try:
#         os.makedirs(done_folder, exist_ok=True)  # Ensure the "done" folder exists
#         shutil.move(file_path, os.path.join(done_folder, os.path.basename(file_name)))
#
#         # Send Slack alert
#         #message = f":white_check_mark: File `{file_name}` has been processed and loaded into the database."
#         #hook.send(text=message)
#
#         logging.info(f" --  File {file_name} successfully moved and Slack notification sent.")
#
#     except PermissionError:
#         logging.error(f"Permission denied: Cannot move {file_name} to {done_folder}.")
#         message = f":triangular_flag_on_post: Permission denied: Cannot move {file_name} to {done_folder}."
#         hook.send(text=message)
#     except FileNotFoundError:
#         logging.error(f"File not found: {file_path} does not exist.")
#         message = f":triangular_flag_on_post: File not found: {file_path} does not exist."
#         hook.send(text=message)
#     except Exception as e:
#         logging.error(f"Error moving file {file_name}: {str(e)}")
#         message = f":triangular_flag_on_post: Error moving file {file_name}: {str(e)}"
#         hook.send(text=message)


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
        logging.info("    > SQL connection established.")
        return conn
    except pyodbc.Error as e:
        logging.error(f"Error while connecting to the SQL Server: {e}")
        return None


def sanitize_filename(filename):
    filename = filename.replace(" ", "_")
    filename = re.sub(r'[^a-zA-Z0-9_-]', '', filename)
    if not filename.lower().endswith(".csv"):
        filename += ".csv"
    return filename


def clean_df(df):
    # print(f"Columns before cleaning: {df.columns.tolist()}")
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
        'Location coordinates': 'LocationCoordinates',
        'Device Name': 'device_name',
        'Device Model': 'device_model',
        'Serial Number': 'serial_number',
        'File Name': 'filename',
    }
    df.rename(columns=column_map, inplace=True)
    return df


def extract(**kwargs):
    file_pattern = os.path.join(FOLDER_PATH, '*.csv')
    files = glob.glob(file_pattern)

    if not files:
        logging.error("No CSV files found for extraction.")
        slack_alert(kwargs, success=False)
        raise DataExtractionError("No files found for extraction.")
    malformed = False
    extracted_data = []
    for file in files:

        if os.path.getsize(file) == 0:
            logging.warning(f"Skipping empty file: {file}")
            move_file_to_done(os.path.basename(file), "NoData")
            continue

        with open(file, 'r', encoding='utf-8') as f:
            metadata_lines = [next(f) for _ in range(3)]
        device_name = None
        device_model = None
        serial_number = None
        # logging.info(f"Extracting metadata from file: {metadata_lines}")
        for line in metadata_lines:
            parts = line.strip().split(',')
            if len(parts) < 2:
                print(f"⚠️ Skipping whole file due to malformed row: {file}")
                malformed = True
                break
            key = parts[0].strip().strip('"').lower()
            value = parts[1].strip().strip('"')

            if key == 'device name':
                device_name = value
            elif key == 'device model':
                device_model = value
            elif key == 'serial number':
                serial_number = value

        if malformed:
            continue

        if not serial_number:
            raise ValueError(f"Serial number not found in file {file}")

        with open(file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        header_index = next((idx for idx, line in enumerate(lines) if "FORMATTED DATE_TIME" in line), None)

        if header_index is None:
            raise ValueError(f"Header row not found in file: {file}")

        df = pd.read_csv(file, skiprows=3, dtype=str)
        df = df.iloc[1:].reset_index(drop=True)
        df['Device Name'] = device_name
        df['Device Model'] = device_model
        df['Serial Number'] = serial_number
        df['File Name'] = os.path.basename(file)
        df = clean_df(df)
        logging.info(f"Extracted {df.shape[0]} rows from {file}")
        extracted_data.append({'table': df.to_json(orient='records')})

    kwargs['ti'].xcom_push(key='extracted_data', value=json.dumps(extracted_data))


class DataTransformationError(Exception):
    pass


# Function to fix time format and convert date/time
def fix_datetime_format(df):
    # Convert 'DateTime' column to datetime with error handling
    # df['DateTime'] = pd.to_datetime(df['DateTime'], format='%Y-%m-%d %I:%M:%S %p', errors='raise')
    # # IMPORTANT! 01-05-25 13:20 > 2025-05-01 13:20:00
    df['LogDateTime'] = pd.to_datetime(df['DateTime'], format='%d-%m-%y %H:%M').dt.strftime(
        '%Y-%m-%d %H:%M:%S')
    # Replace '.' with ':' in the time string if necessary
    df['DateTime'] = df['DateTime'].apply(lambda x: str(x).replace('.', ':') if isinstance(x, str) else x)

    # Re-format DateTime to '%Y-%m-%d %H:%M:%S'
    df['DateTime'] = df['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df


def handle_missing_values(df):
    # Step 1: Define invalid markers to standardize as missing
    # invalid_markers = ['--', 'NULL', '']
    # categorical_columns = [
    #     'DataType', 'filename', 'Notes'
    # ]
    #
    # numerical_columns = [
    #     'Temperature', 'WetBulbTemp', 'GlobeTemperature', 'RelativeHumidity',
    #     'BarometricPressure', 'Altitude', 'StationPressure', 'WindSpeed',
    #     'HeatIndex', 'DewPoint', 'DensityAltitude', 'Crosswind', 'Headwind',
    #     'CompassMagneticDirection', 'CompassTrueDirection', 'NWBTemp', 'ThermalWorkLimit',
    #     'WetBulbGlobeTemperature', 'WindChill'
    # ]
    #
    # # Step 2: Replace common invalid markers with actual NaN
    # df.replace(invalid_markers, np.nan, inplace=True)

    # # Step 3: Categorical columns → NaN to None
    # for col in categorical_columns:
    #     if col in df.columns:
    #         df[col] = df[col].where(pd.notna(df[col]), None)
    #
    # # Step 4: Numerical columns → preserve NaN to become SQL NULL (via None)
    # for col in numerical_columns:
    #     if col in df.columns:
    #         df[col] = pd.to_numeric(df[col], errors='raise')  # Ensure numeric type
    #         df[col] = df[col].where(pd.notna(df[col]), None)  # Replace NaN with None
    #         missing_count = df[col].isnull().sum()
    #         if missing_count:
    #             logging.warning(f"Column '{col}' has {missing_count} missing values replaced with NULL (None).")

    # Step 5: Catch-all for any remaining NaN values in the DataFrame
    # df = df.applymap(lambda x: None if pd.isna(x) else x)

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

        logging.info("Executing stored procedure to delete duplicates...")

        # Execute the stored procedure
        cursor.execute("{CALL dbo.DeleteKestrelDataDuplicates}")

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
        logging.info("Error:", e)


def transform(**kwargs):
    ti = kwargs['ti']
    extracted_data_json = ti.xcom_pull(key='extracted_data', task_ids='extract_task')
    extracted_data = json.loads(extracted_data_json) if extracted_data_json else []
    transformed_data = []

    if not extracted_data:
        logging.error("No extracted data found in XCom.")
        slack_alert(kwargs, success=False)
        raise DataTransformationError("No data to transform")

    # Define float columns based on your table schema
    float_columns = [

        'Temperature', 'WetBulbTemp', 'GlobeTemperature', 'RelativeHumidity',
        'BarometricPressure', 'StationPressure', 'WindSpeed', 'HeatIndex',
        'DewPoint', 'Crosswind', 'Headwind', 'NWBTemp', 'ThermalWorkLimit',
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

            }
            df.rename(columns=column_map, inplace=True)

            # Drop first row (after header)
            df.drop(index=0, inplace=True)

            # Parse datetime
            df = fix_datetime_format(df)

            # Handle missing values (from the separate function)
            # df = handle_missing_values(df)
            # Convert float columns safely
            # for col in float_columns:
            #     if col in df.columns:
            #         df[col] = pd.to_numeric(df[col], errors='raise')

            # Append clean records
            transformed_data.extend(json.loads(df.to_json(orient='records')))

    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        slack_alert(kwargs, success=False)
        raise DataTransformationError("Data transformation failed") from e

    ti.xcom_push(key='transformed_data', value=json.dumps(transformed_data))


def load(**kwargs):
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
        if rowcount != 0 and filename:
            logging.info(f"Inserted {rowcount} rows for filename: {filename}")

        df = pd.read_json(StringIO(data_entry['table']), orient='records')

        # Clean datetime & numeric formats
        df = fix_datetime_format(df)

        # --- Clean numeric fields ---
        numeric_cols = [
            'Temperature', 'WetBulbTemp', 'GlobeTemperature', 'RelativeHumidity',
            'BarometricPressure', 'Altitude', 'StationPressure', 'WindSpeed',
            'HeatIndex', 'DewPoint', 'DensityAltitude', 'CrossWind', 'HeadWind',
            'CompassMagneticDirection', 'NWBTemp', 'CompassTrueDirection',
            'ThermalWorkLimit', 'WetBulbGlobeTemperature', 'WindChill'
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert invalid strings to NaN
        df = df.replace({np.nan: None})  # Convert NaN to None for pyodbc

        filename = ""

        for _, row in df.iterrows():
            if filename == row['filename']:
                if rowcount == 0:
                    logging.info(f"Processing file: {filename}")
            else:
                if filename:  # ✅ Only move if previous file exists
                    move_file_to_done(os.path.basename(filename))
                    fileCount += 1
                    logging.info(f"Inserted {rowcount} rows for filename: {filename}")
                filename = row['filename']
                logging.info(f"Processing file: {filename}")
                rowcount = 0

            try:
                insert_query = """
                               INSERT INTO kestrel_data (DateTime, Temperature, WetBulbTemp, GlobeTemperature,
                                                         RelativeHumidity, BarometricPressure, Altitude,
                                                         StationPressure, WindSpeed, HeatIndex, DewPoint,
                                                         DensityAltitude, CrossWind, HeadWind, CompassMagneticDirection,
                                                         NWBTemp, CompassTrueDirection, ThermalWorkLimit,
                                                         WetBulbGlobeTemperature, WindChill,
                                                         DataType, device_name, device_model, serial_number, filename)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                               """

                insert_data = tuple(row.get(col, None) for col in [
                    'DateTime', 'Temperature', 'WetBulbTemp', 'GlobeTemperature', 'RelativeHumidity',
                    'BarometricPressure', 'Altitude', 'StationPressure', 'WindSpeed', 'HeatIndex',
                    'DewPoint', 'DensityAltitude', 'CrossWind', 'HeadWind', 'CompassMagneticDirection',
                    'NWBTemp', 'CompassTrueDirection', 'ThermalWorkLimit', 'WetBulbGlobeTemperature',
                    'WindChill', 'DataType', 'device_name', 'device_model', 'serial_number', 'filename'
                ])

                cursor.execute(insert_query, insert_data)
                rowcount += 1

            except pyodbc.IntegrityError as e:
                if "UQ_LogDateTime_DeviceID" in str(e):
                    logging.warning(f"Duplicate entry skipped for file {row['filename']}")
                    continue
                else:
                    logging.error(f"Integrity error in file {row['filename']}: {e}")
                    conn.rollback()
                    raise
            except Exception as e:
                logging.error(f"Error inserting row from file {row['filename']}: {e}")
                conn.rollback()
                raise

        conn.commit()
        if filename:
            move_file_to_done(filename)

    logging.info(f"Data loaded successfully. Processed {fileCount} files.")
    cursor.close()
    delete_duplicates()
    conn.close()


# Define the DAG
dag = DAG(
    'kestrel_5400_data_etl_dag',
    description='Pipeline to process Kestrel weather data and insert into the database',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 6),
    catchup=False,
)

# Define the tasks
wait_for_file_task = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_conn_kestrel",
    filepath="*.csv",
    poke_interval=5,
    timeout=600,
    mode="poke",
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    provide_context=True,
    dag=dag,
    on_success_callback=lambda context: slack_alert(context, success=True)
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
    on_success_callback=lambda context: slack_alert(context, success=True)
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag,
    on_success_callback=lambda context: slack_alert(context, success=True)
)

# Set task dependencies
wait_for_file_task >> extract_task >> transform_task >> load_task
