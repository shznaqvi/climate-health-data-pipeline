import os
import glob
import shutil
import logging
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.dates import days_ago

# Database settings
DB_SERVER = os.getenv('DB_SERVER', '10.1.1.244')
DB_USER = os.getenv('DB_USER', 'ighdapp')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'R8m!zK2@qL')
DB_NAME = os.getenv('DB_NAME', 'ighd')

SLACK_CONN_ID = 'slack_conn'
hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)

FOLDER_PATH = '/opt/airflow/dags/csv/tempu_logsheets/SHAPES'
TEMP_FOLDER = os.path.join(FOLDER_PATH, 'temp')
DONE_FOLDER = os.path.join(FOLDER_PATH, 'done')


def slack_alert(context, status="failed"):
    task_instance = context.get('task_instance')
    airflow_base_url = "http://cls-pae-fl71541:8080/"
    log_url = task_instance.log_url.replace("http://localhost:8080/", airflow_base_url)

    status_map = {
        "success": (":white_check_mark:", "Task Succeeded"),
        "started": (":arrow_forward:", "Task Started"),
        "failed": (":red_circle:", "Task Failed"),
    }
    emoji, status_text = status_map.get(status, (":red_circle:", "Task Failed"))

    slack_msg = f"""
    {emoji} *{status_text}*
    *Task:* `{task_instance.task_id}`
    *DAG:* `{task_instance.dag_id}`
    *Execution Time:* `{context.get('execution_date')}`
    *Log URL:* <{log_url}|View Logs>
    """
    try:
        hook.send(text=slack_msg)
    except Exception as e:
        logging.warning(f"Slack notification failed: {e}")


def stage_excel_to_csv(**kwargs):
    """Converts raw Excel files into staged CSV files for Spark."""
    files = glob.glob(os.path.join(FOLDER_PATH, '*.xlsx'))
    if not files:
        raise FileNotFoundError("No Excel files found for processing.")

    os.makedirs(TEMP_FOLDER, exist_ok=True)
    possible_sheets = ["Tempu Data Log", "Tempu30 Data LogSheet", "Data extract log sheet", "Data Logsheet",
                       "Tempu30 Data LogSheet"]
    staged_files = []

    for file in files:
        logging.info(f"Staging Excel file: {file}")
        xls = pd.ExcelFile(file, engine='openpyxl')
        sheet_to_use = next((s for s in possible_sheets if s in xls.sheet_names), None)

        if not sheet_to_use:
            logging.error(f"No matching logsheet tab found in {file}")
            raise ValueError(f"No matching logsheet tab found in {file}")
            continue

        try:
            df = pd.read_excel(file, sheet_name=sheet_to_use, header=0, skiprows=1, dtype=str)
        except Exception:
            df = pd.read_excel(file, sheet_name=sheet_to_use, header=None, dtype=str)
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)

        df['filename'] = os.path.basename(file)
        temp_csv_path = os.path.join(TEMP_FOLDER, os.path.basename(file).replace('.xlsx', '.csv'))
        df.to_csv(temp_csv_path, index=False)
        staged_files.append(file)

    kwargs['ti'].xcom_push(key='processed_excel_files', value=staged_files)


def cleanup_processed_files(**kwargs):
    """Moves processed Excel files to 'done' and removes temporary CSVs without permission errors."""
    ti = kwargs['ti']
    excel_files = ti.xcom_pull(key='processed_excel_files', task_ids='stage_excel_to_csv_task') or []
    os.makedirs(DONE_FOLDER, exist_ok=True)

    for file_path in excel_files:
        if not os.path.exists(file_path):
            continue

        filename = os.path.basename(file_path)
        dest_path = os.path.join(DONE_FOLDER, filename)

        try:
            # Bypass metadata/timestamp copying that causes permission errors on Docker mounts
            shutil.copyfile(file_path, dest_path)
            os.remove(file_path)
            logging.info(f"Moved {filename} to {DONE_FOLDER}")
        except Exception as e:
            logging.error(f"Error moving {filename}: {e}")
            raise

    # Clean up temp CSV folder
    temp_csvs = glob.glob(os.path.join(TEMP_FOLDER, "*.csv"))
    for temp_csv in temp_csvs:
        try:
            os.remove(temp_csv)
            logging.info(f"Deleted temporary file: {temp_csv}")
        except OSError as e:
            logging.warning(f"Could not delete temp CSV {temp_csv}: {e}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    'on_failure_callback': slack_alert
}

dag = DAG(
    "tempu_logsheet_shapes_etl_dag_spark",
    description='Spark ETL Pipeline for TempU Logsheets',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2025, 1, 7),
    catchup=False,
)

wait_for_file_task = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_conn_tempu_logsheets_shapes",
    filepath="*.xlsx",
    dag=dag,
    poke_interval=10,
    timeout=600,
    mode="poke",
)

stage_excel_to_csv_task = PythonOperator(
    task_id='stage_excel_to_csv_task',
    dag=dag,
    python_callable=stage_excel_to_csv,
    on_execute_callback=lambda context: slack_alert(context, status="started"),
    on_success_callback=lambda context: slack_alert(context, status="success"),
    on_failure_callback=lambda context: slack_alert(context, status="failed"),
)

spark_load_task = BashOperator(
    task_id='spark_load_task',
    dag=dag,
    bash_command=(
        'python /opt/airflow/dags/spark_scripts/spark_process_tempu_logsheet_SHAPES.py '
        '--input-dir /opt/airflow/dags/csv/tempu_logsheets/SHAPES/temp '
        '--db-server 10.1.1.244 '
        '--db-name ighd '
        '--db-user ighdapp '
        '--db-password "R8m!zK2@qL"'
    ),
    on_execute_callback=lambda context: slack_alert(context, status="started"),
    on_success_callback=lambda context: slack_alert(context, status="success"),
    on_failure_callback=lambda context: slack_alert(context, status="failed"),
)

cleanup_task = PythonOperator(
    task_id='cleanup_task',
    dag=dag,
    python_callable=cleanup_processed_files,
    on_execute_callback=lambda context: slack_alert(context, status="started"),
    on_success_callback=lambda context: slack_alert(context, status="success"),
    on_failure_callback=lambda context: slack_alert(context, status="failed"),
)

wait_for_file_task >> stage_excel_to_csv_task >> spark_load_task >> cleanup_task
