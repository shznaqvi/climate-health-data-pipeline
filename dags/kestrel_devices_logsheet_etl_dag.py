import glob
import io
import json
import logging
import os
import shutil
import site

import psutil
# import openpyxl

import pandas as pd
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
import pendulum
import sys
import subprocess

# Testing
# Folder path containing CSV files
FOLDER_PATH = '/opt/airflow/dags/csv/kestrel_logsheets'
TEMP_FOLDER = os.path.join(FOLDER_PATH, "temp")
DONE_FOLDER = os.path.join(FOLDER_PATH, "done")
# print(openpyxl.__version__)
# Database connection details (use environment variables for security)
DB_SERVER = os.getenv('DB_SERVER', '10.1.1.244')
DB_USER = os.getenv('DB_USER', 'ighdapp')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'R8m!zK2@qL')
DB_NAME = os.getenv('DB_NAME', 'ighd')

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the Slack webhook hook as a global variable
hook = SlackWebhookHook(slack_webhook_conn_id="slack_conn")


def slack_alert(context, success=False):
    task_instance = context.get('task_instance')

    # Replace "localhost" with the correct domain
    airflow_base_url = "http://cls-pae-fl71541:8080/"  # Your correct domain
    log_url = task_instance.log_url.replace("http://localhost:8080/", airflow_base_url)

    if success:
        emoji = ":white_check_mark:"
        status = "Task Succeeded in Airflow! ✅"
    else:
        emoji = ":red_circle:"
        status = "Task Failed in Airflow! ❌"

    slack_msg = f"""
    {emoji} *{status}*
    *Task:* `{task_instance.task_id}`
    *DAG:* `{task_instance.dag_id}`
    *Execution Time:* `{context.get('execution_date')}`

    """

    hook.send(text=slack_msg)


def get_sql_connection():
    logging.info("Getting SQL Connection.")
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={DB_SERVER};'
        f'DATABASE={DB_NAME};'
        f'UID={DB_USER};'
        f'PWD={DB_PASSWORD};'
        'Encrypt=no;'
    )
    try:
        conn = pyodbc.connect(conn_str, timeout=10)
        logging.info("SQL Connection established successfully.")
        return conn
    except pyodbc.Error as e:
        logging.error(f"SQL Connection error: {e}")
        message = f"SQL Connection error: {e}"
        hook.send(text=message)
        raise


# def get_package_location(package_name):
#     result = subprocess.run(['pip', 'show', package_name], stdout=subprocess.PIPE)
#     output = result.stdout.decode('utf-8')
#     for line in output.splitlines():
#         if line.startswith('Location'):
#             return line.split(':')[1].strip()
#
#
# def install_package(package):
#     """Install a package using pip."""
#     subprocess.check_output([sys.executable, "-m", "pip", "install", package])


# Extract function
def extract(**kwargs):
    # package_name = 'openpyxl'
    # location = get_package_location(package_name)
    # logger.info(f'{package_name} is installed at: {location} ')
    # try:
    #     import openpyxl
    #     logger.info(f"openpyxl is already installed. - openpyxl.__file__: {openpyxl.__file__}")
    #     logger.info(f"site.getusersitepackages():  {site.getusersitepackages()}")
    #     subprocess.check_output([sys.executable, "-m", "pip", "show", 'openpyxl'])
    # except ImportError:
    #     logger.info("openpyxl is not installed. Installing now...")
    #     install_package('openpyxl')
    #     logger.info("openpyxl has been installed successfully.")

    # file_path = '/opt/airflow/dags/csv/kestrel_logsheets/Copy of Ibuttions Log Sheet_Suhrab Solangi_30042025_HN.xlsx'
    # if os.path.exists(file_path):
    #     logger.info(f"File exists: {file_path}")
    # else:
    #     logger.info(f"File not found: {file_path}")
    # try:
    #     logger.info(f"sys.executable: {sys.executable}")
    #     logger.info(f"sys.path: {sys.path}")
    #     # Log the current user and working directory using psutil
    #     current_user = psutil.Process(os.getpid()).username()
    #     working_directory = os.getcwd()
    #     logger.info(f"Current User: {current_user}")
    #     logger.info(f"Working Directory: {working_directory}")
    #     import getpass
    #     logger.info(f"Current User: {getpass.getuser()}")
    #     logger.info(f"Airflow using Python: {sys.executable}")
    #     # Log the permissions of the target folder
    #     # folder_permissions = oct(os.stat(FOLDER_PATH).st_mode)[-3:]
    #     # logger.info(f"Permissions for {FOLDER_PATH}: {folder_permissions}")

    files = glob.glob(os.path.join(FOLDER_PATH, '*.xlsx'))

    if not files:
        message = ":warning: No Kestrel Excel files found."
        hook.send(text=message)
        raise DataExtractionError("No Excel files found.")

    filenames = [os.path.basename(f) for f in files]

    logging.info(f"Found {len(filenames)} files: {filenames}")

    kwargs['ti'].xcom_push(key='file_list', value=filenames)


# Function to clean column names
def clean_column_names(df):
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_').str.replace('.', '').str.strip()
    rename_map = {
        'kestrel_id': 'device_serial',
        'region': 'region',
        'structure_id': 'structure',
        'paired_structure': 'comparable_structure',
        'intervention_type': 'finalized_interventions',
        'Cluster No': 'cluster_no',
        'temp_type': 'location',  # Changing 'temp_type' to 'surface'
        'temp_location': 'surface',  # Assuming this is retained
        'case/control': 'arm',
        'date-time_of_placement': 'device_placement_date_time',
        'date-time_of_recovery': 'extract_date_time',
        'remarks': 'remarks',
        'file_name': 'data_filename',
        'device_provided_date\n(device_given)': 'device_provided_date',
        'device_removed_date\n(device_taken_back)': 'device_removed_date',
        'final_use_(y/blank)': 'final_use'
    }
    df.rename(columns=rename_map, inplace=True)

    # Define the columns you want to keep
    columns_to_keep = [
        'device_serial', 'region', 'structure', 'comparable_structure',
        'intervention_phase', 'surface', 'location', 'arm',
        'finalized_interventions', 'type', 'device_placement_date_time',
        'extract_date_time', 'remarks', 'filename', 'cluster_no'
    ]

    # Ensure that all the columns exist in the dataframe before attempting to filter
    # existing_columns = [col for col in columns_to_keep if col in df.columns]

    # Filter the dataframe to keep only the desired columns
    # df = df[existing_columns]

    def combine_datetime(date_series, time_series):
        combine_format = '%Y-%m-%d %H:%M:%S'

        # Print date and time series head for debugging
        print(f"Date series head:\n{date_series.head()}")
        print(f"Time series head:\n{time_series.head()}")

        # Only keep rows where both date and time are not NaN
        mask = date_series.notna() & time_series.notna()

        # Initialize result as NaT for all rows
        result = pd.Series(pd.NaT, index=date_series.index)

        # Process only the valid rows
        valid_dates = date_series[mask].astype(str).str[:10]
        valid_times = time_series[mask].astype(str).str.strip()

        # logging.info(f"Combining date and time using {combine_format}")
        # logging.info(f"Valid dates: {valid_dates.head()}")
        # logging.info(f"Valid times: {valid_times.head()}")

        # Assign combined datetime to valid rows
        result[mask] = pd.to_datetime(
            valid_dates + ' ' + valid_times,
            errors='raise',  # coerce invalid formats to NaT
            format=combine_format
        )

        return result

    print(f"Dataframe columns before combining datetime: {df.columns.tolist()}")

    df['device_provided_datetime'] = combine_datetime(df['device_provided_date'], df['device_provided_time'])
    df['device_removed_datetime'] = combine_datetime(df['device_removed_date'], df['device_removed_time'])
    df['data_extracted_datetime'] = combine_datetime(df['data_extracted_date'], df['data_extracted_time'])

    # Drop original date and time columns
    df.drop(columns=[
        'device_provided_date', 'device_provided_time',
        'device_removed_date', 'device_removed_time',
        'data_extracted_date', 'data_extracted_time'
    ], inplace=True)

    # Convert combined datetime columns to string format with milliseconds
    for col in ['device_provided_datetime', 'device_removed_datetime', 'data_extracted_datetime']:
        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]  # Cut microseconds to milliseconds

    return df


# Function to fix time format (replacing dots with colons)
def fix_time_format(time_str):
    if isinstance(time_str, str):
        return time_str.replace('.', ':')
    return time_str


# Function to convert date format
def convert_date_format(date_str):
    if isinstance(date_str, str):
        try:
            return pd.to_datetime(date_str, format='%d-%m-%Y').strftime('%d-%m-%y')
        except ValueError as e:
            message = f"Invalid date format: {date_str} - {str(e)}"
            hook.send(text=message)
            return date_str
    return date_str


# Function to adjust device ID
def adjust_device_id(device_id):
    if pd.isna(device_id):
        return None
    device_id = str(device_id).strip()
    if len(device_id) > 3:
        if not device_id.startswith("11"):
            device_id = "11" + device_id.lstrip("1")
        if not device_id.endswith("53"):
            device_id = device_id.rstrip("53") + "53"
    return device_id


# Main transformation function
def transform(**kwargs):
    ti = kwargs['ti']
    filenames = ti.xcom_pull(key='file_list', task_ids='extract')

    if not filenames:
        raise DataTransformationError("No files received for transformation.")

    os.makedirs(TEMP_FOLDER, exist_ok=True)
    transformed_files = []

    possible_sheets = [
        "Kestrel Heat", "kestrel Data Log", "kestrel Log",
        "Drop-2", "Kestrel DROP 2 log", "Kestrel Data Log Sheet"
    ]

    for fname in filenames:
        file_path = os.path.join(FOLDER_PATH, fname)
        logging.info(f"Transforming: {file_path}")

        try:
            xls = pd.ExcelFile(file_path)
            sheet = next((s for s in possible_sheets if s in xls.sheet_names), None)

            if not sheet:
                raise ValueError(f"No valid sheet in {fname}")

            df = pd.read_excel(file_path, sheet_name=sheet, skiprows=1, dtype=str)
            df['filename'] = fname

            df = clean_column_names(df)

            # ---- Final schema alignment (critical)
            df.rename(columns={
                'device_serial': 'device_id',
                'extract_date_time': 'data_extracted_datetime'
            }, inplace=True)

            df['participant_id'] = None
            df['data_filename'] = df['filename']

            if df.empty:
                raise DataTransformationError(f"No data after transform: {fname}")

            temp_csv = os.path.join(TEMP_FOLDER, fname.replace('.xlsx', '.csv'))
            df.to_csv(temp_csv, index=False)

            transformed_files.append(temp_csv)

        except Exception as e:
            logging.error(f"Transform failed for {fname}: {e}")
            hook.send(text=f":red_circle: Transform failed for {fname}: {e}")
            raise

    ti.xcom_push(key='processed_files', value=transformed_files)


class DataExtractionError(Exception):
    """Custom exception for data extraction errors."""
    pass


class DataTransformationError(Exception):
    """Custom exception for data transformation errors."""
    pass


class DataLoadError(Exception):
    """Custom exception for data load errors."""
    pass


def move_file_to_done(file_name):
    done_folder = os.path.join(FOLDER_PATH, "done")
    file_path = os.path.join(FOLDER_PATH, file_name)

    logging.info(f"Moving file {file_name} from {file_path} to {done_folder} folder.")

    try:
        os.makedirs(done_folder, exist_ok=True)  # Ensure the "done" folder exists
        shutil.move(file_path, os.path.join(done_folder, os.path.basename(file_name)))

        # Send Slack alert
        message = f":white_check_mark: File `{file_name}` has been processed and loaded into the database."
        hook.send(text=message)

        logging.info(f"File {file_name} successfully moved and Slack notification sent.")

    except PermissionError:
        logging.error(f"Permission denied: Cannot move {file_name} to {done_folder}.")
        message = f":triangular_flag_on_post: Permission denied: Cannot move {file_name} to {done_folder}."
        hook.send(text=message)
    except FileNotFoundError:
        logging.error(f"File not found: {file_path} does not exist.")
        message = f":triangular_flag_on_post: File not found: {file_path} does not exist."
        hook.send(text=message)
    except Exception as e:
        logging.error(f"Error moving file {file_name}: {str(e)}")
        message = f":triangular_flag_on_post: Error moving file {file_name}: {str(e)}"
        hook.send(text=message)


def load(**kwargs):
    ti = kwargs['ti']
    csv_files = ti.xcom_pull(key='processed_files', task_ids='transform')

    if not csv_files:
        raise DataLoadError("No transformed CSV files to load.")

    conn = get_sql_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True

    insert_sql = """
                 INSERT INTO kestrel_data_logsheet (region,
                                                    device_id,
                                                    participant_id,
                                                    device_provided_datetime,
                                                    device_removed_datetime,
                                                    final_use,
                                                    data_filename,
                                                    remarks,
                                                    filename,
                                                    data_extracted_datetime)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 """

    for csv in csv_files:
        df = pd.read_csv(csv)

        records = [
            (
                row.region,
                row.device_id,
                row.participant_id,
                row.device_provided_datetime,
                row.device_removed_datetime,
                row.final_use,
                row.data_filename,
                row.remarks,
                row.filename,
                row.data_extracted_datetime
            )
            for row in df.itertuples(index=False)
        ]

        cursor.executemany(insert_sql, records)
        conn.commit()

        logging.info(f"Loaded {len(records)} rows from {csv}")

        # Move original Excel file
        original_file = os.path.basename(csv).replace('.csv', '.xlsx')
        move_file_to_done(original_file)

    conn.close()


# DAG default arguments
# Define default arguments


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.today('UTC').add(days=-1),  # Airflow 3.0 recommended approach
    "retries": 1,
    'on_failure_callback': slack_alert
}

# Define the DAG
dag = DAG(
    'kestrel_devices_logsheet_etl_dag',
    default_args=default_args,
    schedule='@daily',  # Updated: changed from schedule_interval
    catchup=False
)

# Tasks
# Define the wait for file task
wait_for_file = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_conn_kestrel_logsheets",  # Make sure this connection exists in Airflow
    filepath="*.xlsx",  # Adjust based on your file type
    poke_interval=30,  # Check every 30 seconds
    timeout=600,  # Timeout after 10 minutes
    mode="poke",
    dag=dag  # Added: explicitly pass dag
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    # Removed: provide_context=True (deprecated in Airflow 2.0+)
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    # Removed: provide_context=True (deprecated in Airflow 2.0+)
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    # Removed: provide_context=True (deprecated in Airflow 2.0+)
    dag=dag
)

# Task dependencies
wait_for_file >> extract_task >> transform_task >> load_task
