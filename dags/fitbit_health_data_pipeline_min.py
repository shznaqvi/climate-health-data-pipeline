import os
import json
import base64
import requests
import pyodbc
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging = LoggingMixin().log  # Use Airflow's logger

# Production Database
DB_SERVER = os.getenv('DB_SERVER', '10.1.1.244')
DB_USER = os.getenv('DB_USER', 'ighdapp')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'R8m!zK2@qL')
DB_NAME = os.getenv('DB_NAME', 'ighd')

# Folder path containing CSV files
FOLDER_PATH = '/opt/airflow/dags/csv/json'

# Define the Slack webhook hook as a global variable
hook = SlackWebhookHook(slack_webhook_conn_id="slack_conn")

days30 = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


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


def on_failure_callback(context):
    subject = f"Airflow Task Failed: {context['task_instance'].task_id}"
    body = f"Task {context['task_instance'].task_id} failed. Please check the logs."
    send_email("hassan.naqvi@aku.edu", subject, body)
    logging.info(f"Email sent for task {context['task_instance'].task_id} failure.")


# Define the connection to SQL Server
def get_sql_connection():
    logging.info("    > Getting SQL Connection.")
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={DB_SERVER};'
        f'DATABASE={DB_NAME};'
        f'UID={DB_USER};'
        f'PWD={DB_PASSWORD};'
        'Encrypt=no;'  # Disable encryption since it's not a secure server
        'TrustServerCertificate=yes;'
    )
    try:
        conn = pyodbc.connect(conn_str)
        pyodbc.pooling = True
        return conn
    except pyodbc.Error as e:
        logging.error(f" :x: ERROR while connecting to the SQL Server: {e}")
        message = f" :x: ERROR(SQL Connection): Error while connecting to the SQL Server: {e}"
        hook.send_text(message)
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while connecting: {e}")
        message = f" :x: ERROR(SQL Connection): An unexpected error occurred while connecting: {e}"
        hook.send_text(message)
        return None


def save_json(data, filename_prefix):
    """
    Saves any Python data as a JSON file with a timestamped filename.

    Args:
        data (dict or list): The data to save.
        filename_prefix (str): Prefix for the file (e.g., 'activities_data').

    Returns:
        str: Full path to the saved JSON file.
    """

    try:
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M")
        subfolder_path = os.path.join(FOLDER_PATH, filename_prefix)
        os.makedirs(subfolder_path, exist_ok=True)

        filename = f"{filename_prefix}_{timestamp}.json"
        filepath = os.path.join(subfolder_path, filename)

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        logging.info(f"✅ JSON saved to: {filepath}")
        return filepath
    except Exception as e:
        logging.error(f"❌ Failed to save JSON for {filename_prefix}: {e}")
        raise


# Function to refresh access token using refresh token
def refresh_access_token(refresh_token, context, fitbit_user_id):
    client_id = "23PL52"  # Your Fitbit client ID
    client_secret = "6407ab0c90917ee8a7e4cfa7da694027"  # Your Fitbit client secret

    url = "https://api.fitbit.com/oauth2/token"
    headers = {
        "Authorization": f"Basic {base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        tokens = response.json()
        return tokens['access_token'], tokens['refresh_token']
    else:
        # e.g response.text = Error refreshing token: {"errors":[{"errorType":"invalid_grant","message":"Refresh token invalid: cb4c377e10fa9b5fa715d63bbd8925f2bc4d252f620c14018fbafd99ba6e8be4. Visit https://dev.fitbit.com/docs/oauth2 for more information on the Fitbit Web API authorization process."}],"success":false}
        logging.error(f" :x: ERROR refreshing token: {response.text}")
        message = f" :x: ERROR(Refreshing token): {refresh_token}"
        hook.send_text(message)
        response_text = response.text
        # Parse the response string into a dictionary
        response_json = json.loads(response_text)
        error = response_json['errors'][0].get('message', 'No message found.')
        message = (
            f":x: *Token Refresh Failed* for Fitbit User *`{fitbit_user_id}`*.\n"
            f"> *Reason:* `{error}`\n"
        )
        hook.send_text(message)
        return None, None


# Function to refresh tokens for all users in the database
def refresh_tokens(**kwargs):
    context = kwargs.get('context', None)
    logging.info("Refreshing tokens for all users...")
    refresh_success = 0
    refresh_fail = 0

    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        # Fetch all tokens from the fitbit_tokens table
        cursor.execute(
            "SELECT fitbit_user_id, refresh_token FROM fitbit_tokens where fitbit_user_id not like '%@aku%' and (is_active is null or is_active = 1)")
        all_tokens = cursor.fetchall()

        if not all_tokens:
            logging.warning("No tokens found in the database.")
            message = "ERROR(Refresh Tokens): No tokens found in the database."
            hook.send_text(message)
            return

        for token_data in all_tokens:
            fitbit_user_id = token_data[0]
            refresh_token = token_data[1]

            logging.info(f"Processing Fitbit User ID: {fitbit_user_id}")

            # Refresh token
            new_access_token, new_refresh_token = refresh_access_token(refresh_token, context, fitbit_user_id)

            if new_access_token and new_refresh_token:
                # Update the tokens in the database
                update_query = """
                               UPDATE fitbit_tokens
                               SET access_token            = ?,
                                   refresh_token           = ?,
                                   authorization_timestamp = ?
                               WHERE fitbit_user_id = ? \
                               """
                cursor.execute(update_query, (new_access_token, new_refresh_token, datetime.now(), fitbit_user_id))
                conn.commit()

                logging.info(f"Tokens refreshed for Fitbit User ID: {fitbit_user_id}")
                refresh_success += 1
            else:
                # Handle the case where token refresh fails
                logging.error(f"Failed to refresh tokens for Fitbit User ID: {fitbit_user_id}")
                message = f" :x: ERROR(Refresh Tokens): Failed to refresh tokens for Fitbit User ID: {fitbit_user_id}"
                # hook.send_text(message)
                refresh_fail += 1
        if refresh_success == 0:
            logging.info("No tokens were refreshed.")
            message = " :x: ERROR(Refresh Tokens): No tokens were refreshed."
            hook.send_text(message)
            raise ValueError("No tokens were refreshed.")  # Fail if no tokens were refreshed



    except pyodbc.Error as e:
        logging.error(f" :x: ERROR with SQL connection: {e}")
        message = f" :x: ERROR(Refresh Tokens): SQL connection error: {e}"
        hook.send_text(message)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        message = f" :x: ERROR(Refresh Tokens): Unexpected error occurred: {e}"
        hook.send_text(message)
    finally:
        if conn:
            conn.close()


# Function to fetch tokens and push them to XCom
def fetch_tokens(**kwargs):
    """
    Fetch Fitbit tokens from the database and push them to XCom.
    """
    logging.info("Fetching Tokens...")

    try:
        # Establish database connection
        with get_sql_connection() as conn:
            with conn.cursor() as cursor:
                # Execute query to fetch tokens
                cursor.execute("""
                               SELECT LTRIM(RTRIM(fitbit_user_id)) AS fitbit_user_id,
                                      LTRIM(RTRIM(access_token))   AS access_token
                               FROM fitbit_tokens
                               WHERE CAST(authorization_timestamp AS DATE) = CAST(GETDATE() AS DATE)
                                 AND fitbit_user_id not like '%@aku%'
                                 and (is_active is null or is_active = 1);
                               """)
                tokens = cursor.fetchall()

        if not tokens:
            logging.warning("No tokens returned from the database.")
            message = "ERROR(Fetch Tokens): No tokens returned from the database."
            hook.send_text(message)
            raise ValueError("No tokens found in the database.")  # Fail if no tokens

        token_dict = [
            {"fitbit_user_id": row[0].strip(), "access_token": row[1].strip()} for row in tokens
        ]
        logging.info(f"Fetched {len(tokens)} tokens.")

        # Push tokens to XCom
        kwargs["ti"].xcom_push(key="tokens", value=token_dict)
        logging.info("Pushed tokens to XCom.")

        # return token_dict

    except pyodbc.Error as db_error:
        logging.error(f"Database error: {db_error}")
        message = f" :x: ERROR(Fetch Tokens): Database error occurred: {db_error}"
        hook.send_text(message)
        raise  # Reraise the database error to fail the task
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        message = f" :x: ERROR(Fetch Tokens): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Reraise the exception to fail the task


def get_with_retry(url, headers):
    """Reusable GET request with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=5,  # Retry up to 5 times
        backoff_factor=2,  # Wait 2s, 4s, 8s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        return session.get(url, headers=headers, timeout=15)
    except requests.exceptions.SSLError as ssl_err:
        logging.error(f"SSL Error: {ssl_err}")
        return None


# Function to fetch breathing rate data from Fitbit API

def fetch_breathing_rate_data(**kwargs):
    try:
        ti = kwargs['ti']
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError("No tokens available for API check.")

        all_data = []

        for user_data in tokens:
            fitbit_user_id = user_data.get('fitbit_user_id')
            access_token = user_data.get('access_token')

            if not access_token or len(access_token) < 30:
                logging.warning(f"Invalid or missing token for user {fitbit_user_id}. Skipping.")
                hook.send_text(f":warning: Token missing or invalid for user {fitbit_user_id}. Skipping.")
                continue

            logging.info(f"Fetching breathing rate data for {fitbit_user_id}")

            api_url = f"https://api.fitbit.com/1/user/{fitbit_user_id}/br/date/{days30}/{yesterday}.json"
            headers = {"Authorization": f"Bearer {access_token}"}

            # ✅ Use retry-enabled session instead of bare requests.get()
            response = get_with_retry(api_url, headers)

            if response is None:
                message = f":x: CRITICAL(Fetch Breathing Rate): No response for {fitbit_user_id}."
                hook.send_text(message)
                continue

            logging.info(f"Response status for {fitbit_user_id}: {response.status_code}")

            # ✅ Guard against empty or non-JSON body before calling .json()
            if not response.text.strip():
                logging.error(f"Empty response body for {fitbit_user_id} (status {response.status_code}). Skipping.")
                hook.send_text(
                    f":x: ERROR(Fetch Breathing Rate): Empty body for {fitbit_user_id}, status {response.status_code}.")
                continue

            try:
                data = response.json()
            except ValueError as json_err:
                logging.error(f"JSON decode error for {fitbit_user_id}: {json_err}. Raw: {response.text[:200]}")
                hook.send_text(f":x: ERROR(Fetch Breathing Rate): JSON decode failed for {fitbit_user_id}.")
                continue

            if response.status_code == 200:
                if 'br' in data:
                    all_data.append({
                        "fitbit_user_id": fitbit_user_id,
                        "br": data['br']
                    })
                else:
                    logging.error(f"No 'br' key in response for {fitbit_user_id}.")
                    hook.send_text(f":x: ERROR(Fetch Breathing Rate): No breathing rate data for {fitbit_user_id}.")
            else:
                error_type = data.get('errors', [{}])[0].get('errorType', 'Unknown')
                logging.error(f"Error fetching data for {fitbit_user_id}: {response.status_code}, type: {error_type}")
                hook.send_text(
                    f":x: ERROR(Fetch Breathing Rate): {fitbit_user_id} got {response.status_code}, {error_type}.")

        if not all_data:
            raise ValueError("No breathing rate data collected for any user.")

        save_json(all_data, "breathing_rate")
        ti.xcom_push(key='breathing_rate_data', value=all_data)

    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        hook.send_text(f":x: ERROR(Fetch Breathing Rate): Request exception: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        hook.send_text(f":x: ERROR(Fetch Breathing Rate): Unexpected error: {e}")
        raise


# Insert the breathing rate data into the database
def insert_breathing_rate_data(**kwargs):
    logging.info("Starting breathing rate data insertion...")

    try:
        ti = kwargs['ti']  # Get task instance
        # Pull the list of skin temperature data from XCom
        breathing_rate_data = ti.xcom_pull(key='breathing_rate_data', task_ids='fetch_breathing_rate_data')

        if not breathing_rate_data:
            raise ValueError("No breathing rate data available to insert.")

        conn = get_sql_connection()
        cursor = conn.cursor()
        insert_count = 0
        update_count = 0
        for user_data in breathing_rate_data:
            fitbit_user_id = user_data['fitbit_user_id']
            breathing_rate_records = user_data['br']

            for day_data in breathing_rate_records:
                date_time = day_data['dateTime']
                breathing_rate = day_data['value'].get('breathingRate')

                if breathing_rate is None:
                    logging.warning(f"Skipping null 'breathing rate' for {fitbit_user_id} on {date_time}.")
                    message = f"Warning(Insert Breathing Rate): Skipping null 'breathing rate' for {fitbit_user_id} on {date_time}."
                    hook.send_text(message)
                    continue

                # Check for duplicate entry (unique constraint check)
                cursor.execute(
                    "SELECT COUNT(*) FROM fitbit_breathing_rate WHERE date_time = ? AND fitbit_user_id = ?",
                    (date_time, fitbit_user_id)
                )
                if cursor.fetchone()[0] > 0:
                    logging.info(f"Duplicate found. Updating breathing rate for {fitbit_user_id} on {date_time}.")
                    cursor.execute(
                        """UPDATE fitbit_breathing_rate
                           SET breathing_rate = ?
                           WHERE date_time = ?
                             AND fitbit_user_id = ?""",
                        (breathing_rate, date_time, fitbit_user_id)
                    )
                    update_count += 1
                else:  # Insert new data
                    logging.info(f"Inserting breathing rate data for {fitbit_user_id} on {date_time}.")

                    cursor.execute(
                        """INSERT INTO fitbit_breathing_rate (date_time, breathing_rate, fitbit_user_id)
                           VALUES (?, ?, ?)""",
                        (date_time, breathing_rate, fitbit_user_id)
                    )
                    insert_count += 1

        conn.commit()
        logging.info(f"Inserted {insert_count} new records and updated {update_count} existing records.")
        logging.info("Breathing rate data insertion completed successfully.")
    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error occurred: {db_err}")
        message = f" :x: ERROR(Insert Breathing Rate): Database error occurred: {db_err}"
        hook.send_text(message)
        raise  # Re-raise the database error to fail the task
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        message = f" :x: ERROR(Insert Breathing Rate): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Re-raise the general error to fail the task
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


# Define the DAG
dag = DAG(
    'fitbit_health_data_pipeline_min',
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=20),
        'on_failure_callback': slack_alert,

    },
    description='A DAG to fetch and process Fitbit data',
    schedule='@daily',  # Updated to 'schedule' instead of 'schedule_interval'
    start_date=datetime(2024, 11, 1),
    catchup=False

)

# Task to refresh tokens using the PHP script
refresh_tokens_task = PythonOperator(
    task_id='refresh_tokens',
    python_callable=refresh_tokens,
    dag=dag
)

# Airflow DAG tasks definition
fetch_tokens_task = PythonOperator(
    task_id='fetch_tokens',
    python_callable=fetch_tokens,
    trigger_rule='all_success',
    dag=dag
)

# Airflow task to fetch breathing rate data for each user
fetch_breathing_rate_data_task = PythonOperator(
    task_id='fetch_breathing_rate_data',
    python_callable=fetch_breathing_rate_data,
    provide_context=True,  # Ensure context is passed to your function
    dag=dag,
)

# Airflow task to insert breathing rate data
insert_breathing_rate_data_task = PythonOperator(
    task_id='insert_breathing_rate_data',
    python_callable=insert_breathing_rate_data,
    dag=dag
)

# Set task dependencies
refresh_tokens_task >> fetch_tokens_task >> fetch_breathing_rate_data_task >> insert_breathing_rate_data_task
