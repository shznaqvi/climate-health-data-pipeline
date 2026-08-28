import glob
import io
import json
import logging
import os
import shutil
import pandas as pd
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the Slack webhook hook
hook = SlackWebhookHook(slack_webhook_conn_id="slack_conn")

# Folder paths
FOLDER_PATH = '/opt/airflow/dags/csv/ibutton_data'
DONE_FOLDER = os.path.join(FOLDER_PATH, 'done')
TEMP_FOLDER = os.path.join(FOLDER_PATH, 'temp')

# Ensure directories exist
os.makedirs(DONE_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)

# Production Database
DB_SERVER = os.getenv('DB_SERVER', '10.1.1.244')
DB_USER = os.getenv('DB_USER', 'ighdapp')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'R8m!zK2@qL')
DB_NAME = os.getenv('DB_NAME', 'ighd')


def slack_alert(context, success=False):
    task_instance = context.get('task_instance')
    airflow_base_url = "http://cls-pae-fl71541:8080/"
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
    *Logs:* <{log_url}|View Logs>
    """
    hook.send(text=slack_msg)


def move_file_to_done(file_name, status_folder="done"):
    target_folder = os.path.join(FOLDER_PATH, status_folder)
    file_path = os.path.join(FOLDER_PATH, file_name)

    try:
        os.makedirs(target_folder, exist_ok=True)
        if os.path.exists(file_path):
            shutil.move(file_path, os.path.join(target_folder, os.path.basename(file_name)))
            logging.info(f"Moved {file_name} to {status_folder}.")
    except Exception as e:
        logging.error(f"Error moving file {file_name}: {e}")
        hook.send(text=f":triangular_flag_on_post: Error moving file {file_name}: {e}")


def get_sql_connection():
    logging.info("Establishing SQL Connection...")
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={DB_SERVER};'
        f'DATABASE={DB_NAME};'
        f'UID={DB_USER};'
        f'PWD={DB_PASSWORD};'
        'Encrypt=no;'
    )
    conn = pyodbc.connect(conn_str, timeout=10)
    return conn


def delete_duplicates():
    try:
        with get_sql_connection() as conn:
            cursor = conn.cursor()
            logging.info("Executing stored procedure to delete duplicates...")
            cursor.execute("{CALL dbo.Delete_Duplicates_LogData_IButtonData}")

            while True:
                messages = cursor.get_messages()
                if not messages:
                    break
                for msg in messages:
                    logging.info(f"SQL_Log: {msg[1]}")

            conn.commit()
            logging.info("Duplicates removed successfully.")
    except Exception as e:
        logging.error(f"Error deleting duplicates: {e}")


def extract(**kwargs):
    file_pattern = os.path.join(FOLDER_PATH, '*.xlsx')
    files = glob.glob(file_pattern) + glob.glob(os.path.join(FOLDER_PATH, '*.xls'))

    if not files:
        logging.info("No files found for extraction.")
        hook.send(text=":warning: *No Files Found!* *Task:* ibutton extract task")
        return []

    extracted_files = []

    for file in files:
        base_name = os.path.basename(file)
        try:
            if os.path.getsize(file) == 0:
                move_file_to_done(base_name, "NoData")
                continue

            file_ext = os.path.splitext(file)[1].lower()
            engine = 'openpyxl' if file_ext == '.xlsx' else 'xlrd'

            df = pd.read_excel(file, header=None, engine=engine)

            if df.empty:
                move_file_to_done(base_name, "NoData")
                continue

            metadata = {'filename': base_name, 'metadata': {}}

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

            if "Date" not in df[0].values:
                move_file_to_done(base_name, "InvalidFormat")
                continue

            data_start_index = df[df[0] == "Date"].index[0] + 1
            table = pd.read_excel(file, header=data_start_index, engine=engine)

            if table.empty:
                move_file_to_done(base_name, "NoData")
                continue

            table = table.drop(table.index[-1]) if not table.empty else table
            table.columns = ['Date', 'Time', 'Value']

            # Write intermediate data to disk to avoid XCom bloat
            temp_output_path = os.path.join(TEMP_FOLDER, f"extracted_{base_name}.json")
            with open(temp_output_path, 'w') as f:
                json.dump({
                    'filename': metadata['filename'],
                    'metadata': metadata['metadata'],
                    'table': table.to_json(orient='records')
                }, f)

            extracted_files.append(temp_output_path)

        except Exception as e:
            logging.error(f"Error reading file {file}: {e}")
            move_file_to_done(base_name, "Error")
            continue

            # Push file paths instead of actual data
    kwargs['ti'].xcom_push(key='extracted_files', value=extracted_files)


def transform(**kwargs):
    ti = kwargs['ti']
    extracted_files = ti.xcom_pull(key='extracted_files', task_ids='extract') or []
    transformed_files = []

    for file_path in extracted_files:
        try:
            with open(file_path, 'r') as f:
                item = json.load(f)

            df = pd.read_json(io.StringIO(item['table']))
            metadata = item['metadata']

            if 'Date' in df.columns and 'Time' in df.columns:
                df['Datetime'] = pd.to_datetime(
                    df['Date'].astype(str) + ' ' + df['Time'].astype(str),
                    format='%Y-%m-%d %H:%M:%S', errors='coerce'
                ).dt.strftime('%Y-%m-%d %H:%M:%S')
                df = df.drop(columns=['Date', 'Time'])

            for key, val in metadata.items():
                df[key] = val

            df['filename'] = item['filename']

            temp_output_path = os.path.join(TEMP_FOLDER, f"transformed_{item['filename']}.json")
            df.to_json(temp_output_path, orient='records')
            transformed_files.append(temp_output_path)

            # Clean up the extracted temp file
            os.remove(file_path)

        except Exception as e:
            logging.error(f"Error transforming data in {file_path}: {e}")
            original_filename = item.get('filename') if 'item' in locals() else "unknown"
            move_file_to_done(original_filename, "Error")
            continue

    ti.xcom_push(key='transformed_files', value=transformed_files)


def load(**kwargs):
    ti = kwargs['ti']
    transformed_files = ti.xcom_pull(key='transformed_files', task_ids='transform') or []

    if not transformed_files:
        logging.info("No transformed files found to load.")
        return

    try:
        with get_sql_connection() as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True  # Critical for pyodbc performance

            query = """
                    INSERT INTO ibutton_data (Datetime, Value, Data_Unit, Mission_Sample_Count, deviceID, filename)
                    VALUES (?, ?, ?, ?, ?, ?) \
                    """

            for file_path in transformed_files:
                try:
                    df = pd.read_json(file_path)
                    if df.empty:
                        continue

                    # Convert dataframe to list of tuples for executemany
                    records = df[['Datetime', 'Value', 'Data_Unit', 'Mission_Sample_Count', 'deviceID',
                                  'filename']].values.tolist()

                    cursor.executemany(query, records)
                    conn.commit()

                    original_filename = df['filename'].iloc[0]
                    logging.info(f"Loaded {len(records)} rows from {original_filename}.")

                    # Move original file to done and cleanup temp
                    move_file_to_done(original_filename, "done")
                    os.remove(file_path)

                except Exception as e:
                    logging.error(f"Failed to load data from {file_path}: {e}")
                    conn.rollback()

    except Exception as e:
        logging.error(f"Database connection/load error: {e}")
        hook.send(text=f"Database load error: {e}")
        raise
    finally:
        delete_duplicates()


# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    'on_failure_callback': slack_alert
}

dag = DAG(
    "ibutton_data_etl_dag_fast",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

wait_for_file = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_conn_ibutton",
    filepath="*.xlsx",
    poke_interval=30,
    timeout=600,
    mode="poke",
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

wait_for_file >> extract_task >> transform_task >> load_task
