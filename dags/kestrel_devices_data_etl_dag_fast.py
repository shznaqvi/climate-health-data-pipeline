import os
import shutil
import glob
import logging
import re
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import numpy as np

logging = LoggingMixin().log

# === DB Config ===
DB_SERVER = os.getenv('DB_SERVER', '10.1.1.244')
DB_USER = os.getenv('DB_USER', 'ighdapp')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'R8m!zK2@qL')
DB_NAME = os.getenv('DB_NAME', 'ighd')

SLACK_CONN_ID = 'slack_conn'
hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)

FOLDER_PATH = '/opt/airflow/dags/csv/kestrel_data'
DONE_FOLDER = os.path.join(FOLDER_PATH, 'done')
TEMP_FOLDER = os.path.join(FOLDER_PATH, 'temp')


# --- Helper Functions ---
def slack_alert(context, status="failed"):
    task_instance = context.get('task_instance')
    airflow_base_url = "http://cls-pae-fl71541:8080/"
    log_url = task_instance.log_url.replace("http://localhost:8080/", airflow_base_url)
    emoji = ":white_check_mark:" if status == "success" else ":arrow_forward:" if status == "started" else ":red_circle:"
    text = "Task Succeeded" if status == "success" else "Task Started" if status == "started" else "Task Failed"
    msg = f"{emoji} *{text}*\n*Task:* `{task_instance.task_id}`\n*DAG:* `{task_instance.dag_id}`\n*Log:* <{log_url}|View>"
    hook.send(text=msg)


def move_file_to_done(file_name, folder="done"):
    done_folder = os.path.join(FOLDER_PATH, folder)
    os.makedirs(done_folder, exist_ok=True)
    src = os.path.join(FOLDER_PATH, file_name)
    dest = os.path.join(done_folder, os.path.basename(file_name))
    shutil.move(src, dest)
    temp = os.path.join(FOLDER_PATH, TEMP_FOLDER, os.path.basename(file_name))
    if os.path.exists(temp): os.remove(temp)


def get_sql_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD};Encrypt=no;"
    )
    return pyodbc.connect(conn_str)


# --- Extraction ---
def extract(**kwargs):
    files = glob.glob(os.path.join(FOLDER_PATH, '*.csv'))
    if not files:
        raise FileNotFoundError("No Kestrel CSV files found.")
    kwargs['ti'].xcom_push(key='file_list', value=files)
    logging.info(f"Found {len(files)} Kestrel files.")


def fix_datetime_format(df, a=None):
    # print(f"Fixing DateTime format...{a}")
    # Check if the column exists
    if 'DateTime' not in df.columns:
        logging.warning("⚠️ 'DateTime' column missing in DataFrame.")
        return df
    # else:
    #     logging.info("✅ 'DateTime' column found in DataFrame.")

    # Try multiple formats safely
    datetime_formats = [
        '%Y-%m-%d %I:%M:%S %p',  # e.g. 2025-09-30 01:40:00 pm
        '%Y-%m-%d %H:%M:%S',  # e.g. 2025-09-30 13:40:00
        '%d-%m-%y %H:%M',  # e.g. 30-09-25 13:40
        '%d/%m/%Y %H:%M',  # e.g. 30/09/2025 13:40
    ]

    parsed = None
    for fmt in datetime_formats:
        try:
            parsed = pd.to_datetime(df['DateTime'], format=fmt, errors='raise')
            logging.debug(f"✅ Parsed DateTime using format: {fmt}")
            break
        except Exception:
            continue

    # If all formats failed, fallback to flexible inference
    if parsed is None:
        logging.warning("⚠️ Falling back to mixed datetime parsing.")
        parsed = pd.to_datetime(df['DateTime'], format='mixed', errors='coerce')

    # Assign back to DataFrame in standard format
    df['DateTime'] = parsed.dt.strftime('%Y-%m-%d %H:%M:%S')
    # df.drop(columns=['DateTime'], inplace=True, errors='ignore')
    return df


# --- Transformation ---
def clean_df(df):
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
    # print(f"Columns after renaming: {df.columns.tolist()}")
    # # Rename columns to match DB schema
    # rename_map = {
    #     'Date': 'LogDate',
    #     'Time': 'LogTime',
    #     'Temperature (°C)': 'Temperature',
    #     'RelativeHumidity (%)': 'RelativeHumidity',
    #     'BarometricPressure (hPa)': 'BarometricPressure',
    #     'StationPressure (hPa)': 'StationPressure',
    #     'Wind Speed (m/s)': 'WindSpeed',
    #     'Heat Index (°C)': 'HeatIndex',
    #     'Dew Point (°C)': 'DewPoint',
    # }
    # df.rename(columns=rename_map, inplace=True)
    #
    # # Rename columns if not already done earlier
    # column_map = {
    #     'FORMATTED DATE_TIME': 'DateTime',
    #     'Wet Bulb Temp': 'WetBulbTemp',
    #     'Globe Temperature': 'GlobeTemperature',
    #     'Relative Humidity': 'RelativeHumidity',
    #     'Barometric Pressure': 'BarometricPressure',
    #     'Station Pressure': 'StationPressure',
    #     'Wind Speed': 'WindSpeed',
    #     'Heat Index': 'HeatIndex',
    #     'Dew Point': 'DewPoint',
    #     'Density Altitude': 'DensityAltitude',
    #     'Compass Magnetic Direction': 'CompassMagneticDirection',
    #     'NWB Temp': 'NWBTemp',
    #     'Compass True Direction': 'CompassTrueDirection',
    #     'Thermal Work Limit': 'ThermalWorkLimit',
    #     'Wet Bulb Globe Temperature': 'WetBulbGlobeTemperature',
    #     'Wind Chill': 'WindChill',
    #     'Data Type': 'DataType',
    #
    # }
    # df.rename(columns=column_map, inplace=True)

    # Combine Date + Time
    df = fix_datetime_format(df, 'a')

    # # Replace missing values
    # df.replace(['--', '', 'NULL'], np.nan, inplace=True)
    # df = df.applymap(lambda x: None if pd.isna(x) else x)
    return df


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
    files = ti.xcom_pull(key='file_list', task_ids='extract_task')
    transformed_files = []

    for file in files:
        malformed = False

        try:
            # --- Skip empty files ---
            if os.path.getsize(file) == 0:
                logging.warning(f"Skipping empty file: {file}")
                move_file_to_done(os.path.basename(file), "NoData")
                continue

            # --- Read first 3 lines for metadata ---
            with open(file, 'r', encoding='utf-8') as f:
                metadata_lines = [next(f) for _ in range(3)]

            device_name = None
            device_model = None
            serial_number = None

            for line in metadata_lines:
                parts = line.strip().split(',')
                if len(parts) < 2:
                    logging.warning(f"⚠️ Skipping malformed metadata line in {file}: {line.strip()}")
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
                move_file_to_done(os.path.basename(file), "Malformed")
                continue

            if not serial_number:
                raise ValueError(f"Serial number not found in file {file}")

            # --- Find header line dynamically ---
            with open(file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            header_index = next(
                (idx for idx, line in enumerate(lines) if "FORMATTED DATE_TIME" in line.upper()),
                None
            )

            if header_index is None:
                raise ValueError(f"Header row not found in file: {file}")

            # --- Read actual CSV data ---
            df = pd.read_csv(file, skiprows=header_index, dtype=str)
            df = df.iloc[1:].reset_index(drop=True)  # skip duplicate header row if present

            # --- Attach metadata ---
            df['Device Name'] = device_name
            df['Device Model'] = device_model
            df['Serial Number'] = serial_number
            df['File Name'] = os.path.basename(file)
            # print(f"df columns before cleaning: {df.columns.tolist()}")
            # --- Clean and normalize ---
            df = clean_df(df)
            # print(f"df columns after cleaning: {df.columns.tolist()}")
            # Convert datetime
            if 'DateTime' in df.columns:
                df['DateTime'] = pd.to_datetime(df['DateTime'], errors='raise')

            missing_markers = ['--', '', 'NULL']
            df.replace(missing_markers, None, inplace=True)
            # Convert numeric columns
            numeric_cols = [
                'Temperature', 'WetBulbTemp', 'GlobeTemperature', 'RelativeHumidity',
                'BarometricPressure', 'StationPressure', 'WindSpeed', 'HeatIndex',
                'DewPoint', 'DensityAltitude', 'NWBTemp', 'ThermalWorkLimit',
                'WetBulbGlobeTemperature', 'WindChill'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='raise')

            # Ensure SQL-friendly missing values
            df = df.where(pd.notna(df), None)

            # df = fix_datetime_format(df, 'b')

            # --- Save transformed file ---
            temp_path = os.path.join(FOLDER_PATH, TEMP_FOLDER, os.path.basename(file))
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)
            df.to_csv(temp_path, index=False)
            transformed_files.append(temp_path)

            logging.info(f"✅ Transformed {file} ({df.shape[0]} rows) with Serial#: {serial_number}")

        except Exception as e:
            logging.error(f"❌ Error transforming {file}: {e}")
            move_file_to_done(os.path.basename(file), "failed")

    ti.xcom_push(key='processed_files', value=transformed_files)


# --- Loading ---
def load(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(key='processed_files', task_ids='transform_task')
    if not files:
        raise ValueError("No transformed files found.")
    conn = get_sql_connection()
    cursor = conn.cursor()
    for file in files:
        df = pd.read_csv(file)
        query = """
                INSERT INTO kestrel_data (DateTime, Temperature, WetBulbTemp, GlobeTemperature, RelativeHumidity, \
                                          BarometricPressure, Altitude, \
                                          StationPressure, WindSpeed, HeatIndex, DewPoint, DensityAltitude, \
                                          CrossWind, HeadWind, CompassMagneticDirection, \
                                          NWBTemp, CompassTrueDirection, ThermalWorkLimit, \
                                          WetBulbGlobeTemperature, WindChill, \
                                          DataType, device_name, device_model, serial_number, filename)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                """
        data = [tuple(row[col] if col in row else None for col in [
            'DateTime', 'Temperature', 'WetBulbTemp', 'GlobeTemperature', 'RelativeHumidity',
            'BarometricPressure', 'Altitude', 'StationPressure', 'WindSpeed', 'HeatIndex',
            'DewPoint', 'DensityAltitude', 'CrossWind', 'HeadWind', 'CompassMagneticDirection',
            'NWBTemp', 'CompassTrueDirection', 'ThermalWorkLimit', 'WetBulbGlobeTemperature',
            'WindChill', 'DataType', 'device_name', 'device_model', 'serial_number', 'filename'
        ]) for _, row in df.iterrows()]
        cursor.fast_executemany = True
        cursor.executemany(query, data)
        conn.commit()
        logging.info(f"✅ Loaded data from {file} into database.({df.shape[0]} rows)")
        move_file_to_done(os.path.basename(file))
    cursor.close()
    delete_duplicates()
    conn.close()


# --- DAG Definition ---
dag = DAG(
    'kestrel_devices_data_etl_dag_fast',
    description='ETL pipeline for Kestrel environmental logger data',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 6),
    catchup=False,
)

# --- Tasks ---
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
    dag=dag,
    provide_context=True,
    on_execute_callback=lambda c: slack_alert(c, "started"),
    on_success_callback=lambda c: slack_alert(c, "success"),
    on_failure_callback=lambda c: slack_alert(c, "failed"),
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    dag=dag,
    provide_context=True,
    on_execute_callback=lambda c: slack_alert(c, "started"),
    on_success_callback=lambda c: slack_alert(c, "success"),
    on_failure_callback=lambda c: slack_alert(c, "failed"),
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    dag=dag,
    provide_context=True,
    on_execute_callback=lambda c: slack_alert(c, "started"),
    on_success_callback=lambda c: slack_alert(c, "success"),
    on_failure_callback=lambda c: slack_alert(c, "failed"),
)

wait_for_file_task >> extract_task >> transform_task >> load_task
