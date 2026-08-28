import os, shutil, pandas as pd, logging, pyodbc
from datetime import datetime
import glob

import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.dates import days_ago

# Display all columns instead of truncating
pd.set_option("display.max_columns", None)

# Display all rows (careful with very large DataFrames)
pd.set_option("display.max_rows", None)

# Set maximum column width (good for long text columns)
pd.set_option("display.max_colwidth", 100)

# Number formatting (e.g., floating precision)
pd.set_option("display.precision", 3)

# Adjust console width
pd.set_option("display.width", 1000)

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

# Slack hook setup
SLACK_CONN_ID = 'slack_conn'
hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)

FOLDER_PATH = '/opt/airflow/dags/csv/tempu_logsheets/ABV'
DONE_FOLDER = os.path.join(FOLDER_PATH, 'done')
TEMP_FOLDER = os.path.join(FOLDER_PATH, 'temp')


def get_sql_connection():
    logging.info("Getting SQL Connection.")
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={DB_SERVER};'
        f'DATABASE={DB_NAME};'
        f'UID={DB_USER};'
        f'PWD={DB_PASSWORD};'
        'Encrypt=no;TrustServerCertificate=yes;'
    )
    try:
        return pyodbc.connect(conn_str, timeout=10)
    except pyodbc.Error as e:
        logging.error(f"SQL Connection error: {e}")
        hook.chat_postMessage(channel="#alerts", text=f":red_circle: SQL Connection error: {e}")
        raise


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
    file_path = os.path.join(FOLDER_PATH, os.path.splitext(file_name)[0] + ".xlsx")
    temp_file = os.path.join(FOLDER_PATH, TEMP_FOLDER, os.path.basename(file_name))

    logging.info(f"\t -- Moving file...")

    try:
        os.makedirs(done_folder, exist_ok=True)  # Ensure the "done" folder exists
        shutil.move(file_path, os.path.join(done_folder, os.path.basename(os.path.splitext(file_name)[0]) + ".xlsx"))

        # ✅ Delete temp file after successful move
        if os.path.exists(temp_file):
            os.remove(temp_file)
            logging.info(f"\t -- Temporary file deleted successfully.")
        else:
            logging.warning(f"*** -> Temporary file not found for deletion.")

        '/opt/***/dags/csv/tempu_logsheets/Tempu Device Logsheets 22-08-2025-Matiari.xlsx'
        message = f":white_check_mark: File `{file_name}` has been processed and moved to 'done'."
        hook.send(text=message)
    except Exception as e:
        logging.error(f"*** --> Error moving file: {str(e)}")
        hook.send(text=f":triangular_flag_on_post: Error moving file {file_name}: {str(e)}")


def clean_dataframe(df):
    # Rename columns to lowercase and replace spaces with underscores
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('(', '')
        .str.replace(')', '')
        .str.replace('/', '_')
        .str.replace('-', '_')
        .str.replace('\n', '_')
    )
    # Column list after initial cleaning
    print(f"Columns after initial cleaning: {df.columns.tolist()}")
    # Rename columns with a dictionary mapping old names to new names
    rename_map = {
        'tempu_device_id': 'device_id',
        'device_removed_date_device_taken_back': 'device_removed_date',
        'final_use_y_blank': 'final_use',
        'date_of_extracted_device_given': 'data_extracted_date',
        'device_provided_date_device_given': 'device_provided_date',
        's.no': 's_no'

    }

    df = df.rename(columns=rename_map)
    # # Show unique values
    # print(f"Show unique values {df['device_provided_date'].unique()}")
    #
    # # Check for actual nulls
    # print(f"Check for actual nulls {df['device_provided_date'].isna().sum()}")
    #
    # # Check for empty strings
    # print(f"Check for empty strings: {(df['device_provided_date'].str.strip() == '').sum()}")
    #
    # # Check for strings like 'nan' (common in Excel CSVs)
    # print(f"Check for 'nan' strings: {(df['device_provided_date'].str.lower() == 'nan').sum()}")

    # print(f"filtered:\n {df[['device_id', 'participant_id', 'device_provided_date', 'device_removed_date', 'device_removed_time', 'data_filename']]}")

    mask = (
            (df['device_removed_date'].fillna("").astype(str).str.strip() != "") &
            (df['device_removed_time'].fillna("").astype(str).str.strip() == "")
    )

    df.loc[mask, 'remarks'] = "Removed: " + df['device_removed_date'].astype(str).str.strip() + " - " + df[
        'remarks'].fillna("")

    # Replace '0' in 'remarks' with empty string (TMK data requirement)
    df['remarks'] = df['remarks'].replace('0', '')

    # Drop rows where 'device_provided_date' is NaN or empty
    df = df[
        (df['device_removed_date'].fillna("").str.strip() != "") |
        (df['data_filename'].fillna("").str.strip() != "") |
        (df['remarks'].fillna("").str.strip() != "")

        ]

    # print(f"df after dropping rows:\n {df[['device_id', 'participant_id', 'device_provided_date', 'device_removed_date', 'device_removed_time', 'data_filename', 'remarks']]}")

    # print(df.info())
    # Define invalid values per type
    invalid_dates = ['1900-01-00', '1900-01-01']
    invalid_times = ['0:00', '00:00:00']
    invalid_zeros = ['0']
    time_columns = ['device_provided_time', 'device_removed_time', 'data_extracted_time']
    # print(f"Df Columns list: {df.columns.tolist()}")
    for col in time_columns:
        df[col] = df[col].str.replace(';', ':', regex=False)

    # Drop rows where any invalid values appear
    df = df[
        ~df['device_provided_date'].isin(invalid_dates) &
        ~df['device_removed_date'].isin(invalid_dates)
        # ~df['data_filename'].isin(invalid_zeros)
        # ~df['data_filename'].isin(invalid_zeros)
        ]
    # Replace invalid values with NaN
    df['device_provided_time'] = df['device_provided_time'].replace(invalid_times, np.nan)
    df['device_removed_time'] = df['device_removed_time'].replace(invalid_times, np.nan)
    df['data_extracted_date'] = df['data_extracted_date'].replace(invalid_dates, np.nan)
    df['data_extracted_time'] = df['data_extracted_time'].replace(invalid_times, np.nan)
    df['remarks'] = df['remarks'].replace(invalid_zeros, np.nan)
    df['data_filename'] = df['data_filename'].replace(invalid_zeros, np.nan)
    # Find columns with no header or unnamed headers
    unnamed_cols = [col for col in df.columns if col.lower().startswith('unnamed')]

    # Check which of these columns are completely empty
    empty_unnamed_cols = [col for col in unnamed_cols if df[col].isna().all() or (df[col] == '').all()]

    # print("Columns with no header and all empty:", empty_unnamed_cols)

    # Drop unwanted columns
    df = df.drop(columns=empty_unnamed_cols, errors='ignore')

    # print(f"Columns in DataFrame before cleaning: {df.columns.tolist()}")

    # Drop unwanted columns
    df = df.drop(columns=['s_no', 'device_qr_code', 'unnamed:_14'], errors='ignore')

    def combine_datetime(date_series, time_series):
        combine_format = '%Y-%m-%d %H:%M:%S'

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
            errors='coerce',  # coerce invalid formats to NaT
            format=combine_format
        )

        return result

    # print(f"Columns in DataFrame after initial cleaning: {df.columns.tolist()}")
    # nan_rows = df[df['Device Provided Date(Device Given)'].isna()]
    # print(nan_rows['device_id', 'participant_id', 'device_provided_date', 'device_provided_time', 'device_removed_date', 'device_removed_time', 'final_use_y_blank', 'data_extracted_date', 'data_extracted_time', 'data_filename', 'remarks',])
    # print(f"DataFrame Info: {df.info()}")
    # print(f" Columns in DataFrame after cleaning: {df.columns}")

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


class DataExtractionError(Exception):
    pass


def extract(**kwargs):
    files = glob.glob(os.path.join(FOLDER_PATH, '*.xlsx'))
    if not files:
        raise DataExtractionError("No Excel files found for extraction.")

    logging.info(f"Found {len(files)} Excel files for extraction.")
    kwargs['ti'].xcom_push(key='file_list', value=files)


class DataTransformationError(Exception):
    # fail task if no data after transformation
    pass


def transform(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(key='file_list', task_ids='extract_task')

    if not files:
        raise DataTransformationError("No files to transform.")

    transformed_files = []

    for file in files:
        logging.info(f"Transforming Excel file: {file}")

        # Read Excel sheet — adjust sheet_name if needed
        try:
            possible_sheets = ["Tempu Data Log", "Data extract log sheet", "Data Logsheet", "Tempu30 Data LogSheet"]

            xls = pd.ExcelFile(file)
            sheet_to_use = next((s for s in possible_sheets if s in xls.sheet_names), None)

            if sheet_to_use:
                df = pd.read_excel(file, sheet_name=sheet_to_use, header=0, skiprows=1, dtype=str)
            else:
                raise ValueError(f"No valid sheet found in {file}. Available sheets: {xls.sheet_names}")
        except Exception as e:
            logging.error(f"Failed to read {file}: {e}")
            message = f":red_circle: Failed to read {file}: {e}"
            hook.send(text=message)
            continue

        # Attach metadata (example: filename as identifier)
        df['filename'] = os.path.basename(file)

        # Custom clean function (you can replace with your real one)
        df = clean_dataframe(df)

        # Save to temp folder
        os.makedirs(os.path.join(FOLDER_PATH, TEMP_FOLDER), exist_ok=True)
        temp_file = os.path.join(FOLDER_PATH, TEMP_FOLDER, os.path.basename(file).replace('.xlsx', '.csv'))
        df.to_csv(temp_file, index=False)

        transformed_files.append(temp_file)
        # raise exception and fail the task if no data after transformation
        if df.empty:
            message = f":red_circle: No data after transformation for all Tempu Logsheet Files: {file}"
            hook.send(text=message)
            raise DataTransformationError(f"No data after transformation for file: {file}")

    ti.xcom_push(key='processed_files', value=transformed_files)


def load(**context):
    ti = context['ti']
    transformed_files = ti.xcom_pull(key='processed_files', task_ids='transform_task') or []
    if not transformed_files:
        raise FileNotFoundError("No transformed CSV found in XCom.")

    for transformed_path in transformed_files:
        df = pd.read_csv(transformed_path)
        logging.info(f"[LOAD] Loading data from: {transformed_path}")

        conn = get_sql_connection()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            try:
                cursor.execute("""
                               INSERT INTO tempu_data_logsheet (region, device_id, participant_id,
                                                                device_provided_datetime, device_removed_datetime,
                                                                final_use, data_extracted_datetime, data_filename,
                                                                remarks, filename)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                               """,
                               *[
                                   None if pd.isna(x) else x
                                   for x in [
                                       row['region'],
                                       row['device_id'],
                                       row['participant_id'],
                                       row['device_provided_datetime'],
                                       row['device_removed_datetime'],
                                       row['final_use'],
                                       row['data_extracted_datetime'],
                                       row['data_filename'],
                                       row['remarks'],
                                       row['filename']
                                   ]
                               ]
                               )

            except pyodbc.IntegrityError as e:
                if "UQ_Device_Participant_Provided" in str(e):
                    logging.warning(
                        f"Duplicate entry skipped (DeviceID: {row['device_id']}, ParticipantID: {row['participant_id']}, ProvidedDateTime: {row['device_provided_datetime']}) in file: {row['filename']}"
                    )
                    continue
            except Exception as e:
                logging.error(f"[LOAD] Insert failed for row {row.to_dict()}: {e}")
                raise

        move_file_to_done(os.path.basename(transformed_path))
        conn.commit()
        cursor.close()
        conn.close()

        logging.info("[LOAD] Data load complete.")

        # Move files to /done
        # for file_path in transformed_files:
        #     move_file_to_done(os.path.basename(file_path))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    'on_failure_callback': slack_alert
}
# DAG Definition
dag = DAG(
    "tempu_logsheet_etl_dag",
    description='Pipeline to process Tempu weather data and insert into the database',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2025, 1, 7),
    catchup=False,
)

# Define the tasks
wait_for_file_task = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_conn_tempu_logsheets_abv",
    filepath="*.xlsx",
    dag=dag,
    poke_interval=5,
    timeout=600,
    mode="poke",

    # /opt/airflow/dags/csv/tempu_data
)

extract_task = PythonOperator(
    task_id='extract_task',
    dag=dag,
    python_callable=extract,
    on_execute_callback=lambda context: slack_alert(context, status="started"),
    on_success_callback=lambda context: slack_alert(context, status="success"),
    on_failure_callback=lambda context: slack_alert(context, status="failed"),
    provide_context=True
)

transform_task = PythonOperator(
    task_id='transform_task',
    dag=dag,

    python_callable=transform,
    on_execute_callback=lambda context: slack_alert(context, status="started"),
    on_success_callback=lambda context: slack_alert(context, status="success"),
    on_failure_callback=lambda context: slack_alert(context, status="failed"),
    provide_context=True
)

load_task = PythonOperator(
    task_id='load_task',
    dag=dag,

    python_callable=load,
    on_execute_callback=lambda context: slack_alert(context, status="started"),
    on_success_callback=lambda context: slack_alert(context, status="success"),
    on_failure_callback=lambda context: slack_alert(context, status="failed"),
    provide_context=True
)

wait_for_file_task >> extract_task >> transform_task >> load_task
