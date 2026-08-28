import glob
import io
import json
import logging
import os
import shutil
import site
import subprocess
import sys

import pandas as pd
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor

# Checking
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the Slack webhook hook as a global variable
hook = SlackWebhookHook(slack_webhook_conn_id="slack_conn")

# Folder path containing CSV files
FOLDER_PATH = '/opt/airflow/dags/csv/ibutton_data'
DONE_FOLDER = os.path.join(FOLDER_PATH, 'done')
TEMP_FOLDER = os.path.join(FOLDER_PATH, 'temp')

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


# 🔔 Slack Alert Function
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


# def move_file_to_done(file_name):
#     done_folder = os.path.join(FOLDER_PATH, "done")
#     file_path = os.path.join(FOLDER_PATH, file_name)

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

    # logging.info(f"Moving file {file_name} from {file_path} to {done_folder} folder.")

    try:
        os.makedirs(done_folder, exist_ok=True)  # Ensure the "done" folder exists
        shutil.move(file_path, os.path.join(done_folder, os.path.basename(file_name)))

        # Send Slack alert
        # message = f":white_check_mark: File `{file_name}` has been processed and loaded into the database."
        # hook.send(text=message)

        logging.info(f" --  File {file_name} successfully moved and Slack notification sent.")

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
        hook.send_text(message)
        raise


def delete_duplicates():
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        print("Executing stored procedure to delete duplicates...")

        # Execute the stored procedure
        cursor.execute("{CALL dbo.Delete_Duplicates_LogData_IButtonData}")

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


class DataExtractionError(Exception):
    """Custom exception for data extraction errors."""
    pass


class DataTransformationError(Exception):
    """Custom exception for data transformation errors."""
    pass


class DataLoadError(Exception):
    """Custom exception for data load errors."""
    pass


def get_package_location(package_name):
    result = subprocess.run(['pip', 'show', package_name], stdout=subprocess.PIPE)
    output = result.stdout.decode('utf-8')
    for line in output.splitlines():
        if line.startswith('Location'):
            return line.split(':')[1].strip()


def install_package(package):
    """Install a package using pip."""
    subprocess.check_output([sys.executable, "-m", "pip", "install", package])


def extract(**kwargs):
    package_name = 'openpyxl'
    location = get_package_location(package_name)
    logger.info(f'{package_name} is installed at: {location} ')
    try:
        import openpyxl
        logger.info(f"openpyxl is already installed. - openpyxl.__file__: {openpyxl.__file__}")
        logger.info(f"site.getusersitepackages():  {site.getusersitepackages()}")
        subprocess.check_output([sys.executable, "-m", "pip", "show", 'openpyxl'])
    except ImportError:
        logger.info("openpyxl is not installed. Installing now...")
        install_package('openpyxl')
        logger.info("openpyxl has been installed successfully.")

    file_pattern = os.path.join(FOLDER_PATH, '*.xlsx')
    files = glob.glob(file_pattern)

    if not files:
        logging.error("No files found for extraction.")
        # Send Slack alert
        message = f":warning: *No Files Found!* *Task:* ibutton extract task *Directory Checked:* ibutton_data"
        hook.send_text(message)
        raise DataExtractionError("No files found for extraction.")

    extracted_data = []

    for file in files:
        try:
            # ✅ Skip empty or unreadable files early
            if os.path.getsize(file) == 0:
                logging.warning(f"Skipping empty Excel file: {file} (0 KB)")
                move_file_to_done(os.path.basename(file), "NoData")
                continue

            # ✅ Determine engine based on file extension
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext == '.xlsx':
                engine = 'openpyxl'
            elif file_ext == '.xls':
                engine = 'xlrd'
            else:
                logging.warning(f"Unsupported Excel file format: {file}")
                move_file_to_done(os.path.basename(file), "InvalidFormat")
                continue

            # ✅ Try reading the file with the right engine
            df = pd.read_excel(file, header=None, engine=engine)

            if df.empty:
                logging.warning(f"File {file} contains no data rows.")
                move_file_to_done(os.path.basename(file), "NoData")
                continue

            metadata = {
                'filename': os.path.basename(file),
                'metadata': {}
            }

            for _, row in df.iterrows():
                if pd.isnull(row[0]) or 'Date' in str(row[0]):
                    break
                if isinstance(row[0], str):
                    if 'Data Unit:' in row[0]:
                        metadata['metadata']['Data_Unit'] = row[2] if not pd.isnull(row[2]) else None
                    elif 'Mission Sample Count:' in row[0]:
                        metadata['metadata']['Mission_Sample_Count'] = row[2] if not pd.isnull(row[2]) else None
                    elif 'Device Serial Number:' in row[0]:
                        metadata['metadata']['deviceID'] = (
                            row[2][-8:] if isinstance(row[2], str) and row[2].startswith('*') else None
                        )

            # ✅ Safely locate data section
            if "Date" not in df[0].values:
                logging.warning(f"No 'Date' header found in {file}. Skipping file.")
                move_file_to_done(os.path.basename(file), "InvalidFormat")
                continue

            data_start_index = df[df[0] == "Date"].index[0] + 1
            table = pd.read_excel(file, header=data_start_index, engine=engine)

            if table.empty:
                logging.warning(f"No data rows found after header in {file}.")
                move_file_to_done(os.path.basename(file), "NoData")
                continue

            table = table.drop(table.index[-1]) if not table.empty else table
            table.columns = ['Date', 'Time', 'Value']

            extracted_data.append({
                'filename': metadata['filename'],
                'metadata': metadata['metadata'],
                'table': table.to_json(orient='records')
            })

            # ✅ Move after successful processing
            # move_file_to_done(os.path.basename(file), "Done")

        except Exception as e:
            logging.error(f"Error reading file {file}: {e}")
            message = f"Error reading file {file}: {e}"
            hook.send_text(message)
            move_file_to_done(os.path.basename(file), "Error")
            continue  # ✅ Do not raise, just skip the bad file

    kwargs['ti'].xcom_push(key='extracted_data', value=json.dumps(extracted_data))


def transform(**kwargs):
    ti = kwargs['ti']
    extracted_data_json = ti.xcom_pull(key='extracted_data', task_ids='extract')
    extracted_data = json.loads(extracted_data_json) if extracted_data_json else []
    transformed_data = []

    try:
        for item in extracted_data:
            df = pd.read_json(io.StringIO(item['table']))
            metadata = item['metadata']

            if 'Date' in df.columns and 'Time' in df.columns:
                df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str),
                                                format='%Y-%m-%d %H:%M:%S', errors='coerce').dt.strftime(
                    '%Y-%m-%d %H:%M:%S')
                df = df.drop(columns=['Date', 'Time'])

            for key in metadata:
                df[key] = metadata[key]

            df['filename'] = item['filename']
            transformed_data.extend(json.loads(df.to_json(orient='records')))

            # Move the file to the 'done' folder after successful processing
            # move_file_to_done(df['filename'].iloc[0])
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        message = f"Error transforming data: {e}"
        logging.error(f"❌ Error transforming {file}: {e}")
        move_file_to_done(os.path.basename(file), "failed")
        hook.send_text(message)
        raise DataTransformationError("Failed to transform data") from e

    ti.xcom_push(key='transformed_data', value=json.dumps(transformed_data))


def load(**kwargs):
    ti = kwargs['ti']
    transformed_data_json = ti.xcom_pull(key='transformed_data', task_ids='transform')
    transformed_data = pd.DataFrame(json.loads(transformed_data_json)) if transformed_data_json else pd.DataFrame()

    if transformed_data.empty:
        logging.warning("No transformed data found. Skipping database load.")
        return

    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        fileCount = 0
        filename = transformed_data['filename'].iloc[0] if not transformed_data.empty else None
        rowcount = 0
        for _, row in transformed_data.iterrows():
            if filename == row['filename']:
                if rowcount == 0:
                    logging.info(f"Processing file: {filename}")
            else:
                logging.info(f"Inserted {rowcount} rows for filename: {filename}")
                move_file_to_done(filename)
                fileCount += 1
                filename = row['filename']
                logging.info(f"Processing file: {filename}")
                rowcount = 0
            query = """
                    INSERT INTO ibutton_data (Datetime, Value, Data_Unit, Mission_Sample_Count, deviceID, filename)
                    VALUES (?, ?, ?, ?, ?, ?) \
                    """
            params = (
                row['Datetime'], row['Value'], row.get('Data_Unit'), row.get('Mission_Sample_Count'),
                row.get('deviceID'),
                row['filename'])

            try:
                cursor.execute(query, params)
                rowcount += 1
            except pyodbc.Error as e:
                logging.error(f"Error executing query: {query} with params: {params} - {e}")
                message = f"Error executing query: {query} with params: {params} - {e}"
                hook.send_text(message)
                raise DataLoadError(f"Failed to execute insert query for row: {row}") from e

        conn.commit()
        cursor.close()
        move_file_to_done(filename)
        logger.info(f"Data loaded successfully. Processed '{fileCount}' files")
        # for filename in filenames:
        #  move_file_to_done(filename)
    except pyodbc.Error as e:
        logging.error(f"Database load error: {e}")
        message = f"Database load error: {e}"
        hook.send_text(message)
        raise DataLoadError("Failed to load data into the database") from e
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        message = f"An unexpected error occurred: {e}"
        hook.send_text(message)
        raise DataLoadError("An unexpected error occurred during data load") from e
    finally:

        if 'conn' in locals():
            delete_duplicates()
            conn.close()
            logging.info("Database connection closed.")


# default_args = {'owner': 'airflow', 'start_date': days_ago(1), 'retries': 1,
#                 'on_failure_callback': slack_alert}  # 👈 Triggers Slack alert on failure
#
# dag = DAG('ibutton_data_etl_dag', default_args=default_args, description='ETL process for iButton data',
#           schedule_interval='@daily')

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    'on_failure_callback': slack_alert
}

# Define DAG object
dag = DAG(
    "ibutton_data_etl_dag",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# Define the wait for file task
wait_for_file = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_conn_ibutton",  # Make sure this connection exists in Airflow
    filepath="*.xlsx",  # Adjust based on your file type
    poke_interval=30,  # Check every 30 seconds
    timeout=600,  # Timeout after 10 minutes
    mode="poke",
)

extract_task = PythonOperator(task_id='extract', python_callable=extract, provide_context=True, dag=dag,
                              on_success_callback=lambda context: slack_alert(context, success=True))
transform_task = PythonOperator(task_id='transform', python_callable=transform, provide_context=True, dag=dag,
                                on_success_callback=lambda context: slack_alert(context, success=True))
load_task = PythonOperator(task_id='load', python_callable=load, provide_context=True, dag=dag,
                           on_success_callback=lambda context: slack_alert(context, success=True))

wait_for_file >> extract_task >> transform_task >> load_task
