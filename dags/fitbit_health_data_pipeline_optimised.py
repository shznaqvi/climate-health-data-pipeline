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
from airflow.models import Variable

logging = LoggingMixin().log  # Use Airflow's logger

AIRFLOW_BASE_URL = "http://cls-pae-fl71541:8080/"  # Your correct domain
FITBIT_BASE_URL = "https://api.fitbit.com/"  # Your correct domain
FITBIT_API_ENDPOINT = {
    "SKIN_TEMP": "/temp/skin/date/{days30}/{yesterday}.json",
    "BREATHING_RATE": "/br/date/{days30}/{yesterday}.json",
    "SLEEP": "/sleep/date/{days30}/{yesterday}.json",
    "CARDIO_SCORE": "/cardioscore/date/{days30}/{yesterday}.json",
    "HEART_RATE_VARIABILITY": "/hrv/date/{days30}/{yesterday}.json",
    "HEART_RATE": "/activities/heart/date/{days30}/{yesterday}.json",
    "ACTIVITIES": "/activities/{activity_key}/date/{days30}/{yesterday}.json"
}  # Your correct domain

# Fetch the database connection details from environment variables


DB_SERVER = Variable.get("DB_SERVER", default_var="host.docker.internal")
DB_USER = Variable.get("DB_USER", default_var="sa")
DB_PASSWORD = Variable.get("DB_PASSWORD", default_var="YourStrong!Passw0rd")
DB_NAME = Variable.get("DB_NAME", default_var="heaps-pilot")

# Define the Slack webhook hook as a global variable
hook = SlackWebhookHook(slack_webhook_conn_id="slack_conn")

days30 = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


# 🔔 Slack Alert Function
def slack_alert(context, success=False):
    task_instance = context.get('task_instance')

    # Replace "localhost" with the correct domain
    log_url = task_instance.log_url.replace("http://localhost:8080/", AIRFLOW_BASE_URL)

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
        'Encrypt=no;'
    )
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        logging.error(f" :x: ERROR while connecting to the SQL Server: {e}")
        hook.send_text(f" :x: ERROR(SQL Connection): {e}")
        raise


def batch_insert(cursor, table_name, data, columns):
    placeholders = ", ".join(["?"] * len(columns))
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    cursor.executemany(query, data)


def make_api_request(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        hook.send_text(f" :x: API Request Failed: {e}")
        raise


# Function to refresh access token using refresh token
def refresh_access_token(refresh_token, context, fitbit_user_id):
    client_id = "23PL52"  # Your Fitbit client ID
    client_secret = "6407ab0c90917ee8a7e4cfa7da694027"  # Your Fitbit client secret

    url = "{FITBIT_BASE_URL}oauth2/token"
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
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        # Fetch all tokens from the fitbit_tokens table
        cursor.execute(
            "SELECT fitbit_user_id, refresh_token FROM fitbit_tokens where is_active is null or is_active = 1")
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
            else:
                # Handle the case where token refresh fails
                logging.error(f"Failed to refresh tokens for Fitbit User ID: {fitbit_user_id}")
                message = f" :x: ERROR(Refresh Tokens): Failed to refresh tokens for Fitbit User ID: {fitbit_user_id}"
                # hook.send_text(message)


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
                                   AND
                                     is_active is null
                                  or is_active = 1;
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


def fetch_skin_temperature_data(**kwargs):
    """
    Fetch skin temperature data from the Fitbit API for the past 30 days
    for each user token retrieved from XCom.
    """
    # logging.info("Fetching skin temperature data...")

    try:
        # Pull the tokens from XCom using the key 'tokens'
        ti = kwargs['ti']  # Get task instance
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError("No tokens available for API check.")
        # logging.info(f"Pulled tokens: {tokens}")

        all_data = []

        # Loop through all tokens (user data)
        for user_data in tokens:
            fitbit_user_id = user_data.get('fitbit_user_id').strip()
            access_token = user_data.get('access_token').strip()

            logging.info(f"Fetching skin temperature data for {fitbit_user_id}")

            api_url = (
                f"{FITBIT_BASE_URL}1/user/{fitbit_user_id}{FITBIT_API_ENDPOINT['SKIN_TEMP']}"
            )
            # logging.info(f'API URL: {api_url}')

            headers = {"Authorization": f"Bearer {access_token}"}

            response = requests.get(api_url, headers=headers)
            logging.info(f"API Response for user {fitbit_user_id}: {response.status_code}")

            # Parse response
            data = response.json()
            if response.status_code == 200:
                if 'tempSkin' in data:
                    if not data['tempSkin']:
                        logging.warning(f"No skin temperature data for user {fitbit_user_id}.")
                        message = f":x: ERROR(Fetch Skin Temperature): No skin temperature data for user {fitbit_user_id}."
                        hook.send_text(message)
                        continue
                    all_data.append({"fitbit_user_id": fitbit_user_id, "tempSkin": data['tempSkin']})
                else:
                    logging.warning(f"No skin temperature data for user {fitbit_user_id}.")
                    message = f" :x: ERROR(Fetch Skin Temperature):No skin temperature data for user {fitbit_user_id}."
                    hook.send_text(message)
            else:
                error_type = data.get('errors', [{}])[0].get('errorType', 'Unknown')
                logging.error(
                    f" :x: ERROR fetching data for user {fitbit_user_id}: {response.status_code}, "
                    f" :x: ERROR Type: {error_type}"
                )
                message = f":x: ERROR(Fetch Skin Temperature):Error fetching data for user {fitbit_user_id}: {response.status_code}, Error Type: {error_type}"
                hook.send_text(message)

        # Check if any data was collected
        if not all_data:
            raise ValueError("No skin temperature data collected for any user.")

        # Push the gathered data to XCom
        ti.xcom_push(key='skin_data', value=all_data)
        logging.info("Skin temperature data pushed to XCom: {all_data}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        message = f" :x: ERROR(Fetch Skin Temperature): Request exception occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        message = f" :x: ERROR(Fetch Skin Temperature): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task


def insert_skin_temperature_data(**kwargs):
    """
    Inserts skin temperature data into the database.
    Fetches data from XCom, checks for duplicates, and inserts only valid records.
    Fails the task if any database errors occur.
    """
    logging.info("Starting skin temperature data insertion...")

    try:
        ti = kwargs['ti']  # Get task instance
        # Pull the list of skin temperature data from XCom
        skin_data = ti.xcom_pull(key='skin_data', task_ids='fetch_skin_temperature_data')

        if not skin_data:
            raise ValueError("No skin temperature data available to insert.")

        conn = get_sql_connection()
        cursor = conn.cursor()
        insert_count = 0
        update_count = 0

        for user_data in skin_data:
            fitbit_user_id = user_data['fitbit_user_id']
            temp_skin_records = user_data['tempSkin']

            for day_data in temp_skin_records:
                date_time = day_data['dateTime']
                log_type = day_data['logType']
                nightly_relative = day_data['value'].get('nightlyRelative')

                if nightly_relative is None:
                    logging.warning(f"Skipping null 'nightlyRelative' for {fitbit_user_id} on {date_time}.")
                    message = f"Warning(Insert Skin Temperature): Skipping null 'nightlyRelative' for {fitbit_user_id} on {date_time}."
                    hook.send_text(message)
                    continue

                # Check for duplicate entries
                cursor.execute(
                    "SELECT COUNT(*) FROM fitbit_skin_temperature_data WHERE datetime = ? AND fitbit_user_id = ?",
                    (date_time, fitbit_user_id)
                )
                if cursor.fetchone()[0] > 0:
                    logging.info(f"Duplicate detected for {fitbit_user_id} on {date_time}. Updating record.")
                    cursor.execute(
                        """
                        UPDATE fitbit_skin_temperature_data
                        SET nightlyRelative = ?,
                            logType         = ?
                        WHERE datetime = ?
                          AND fitbit_user_id = ?
                        """,
                        (nightly_relative, log_type, date_time, fitbit_user_id)
                    )
                    update_count += 1
                else:
                    # Insert the record into the database
                    logging.info(f"Inserting skin temperature data for {fitbit_user_id} on {date_time}.")
                    cursor.execute(
                        """
                        INSERT INTO fitbit_skin_temperature_data (datetime, nightlyRelative, fitbit_user_id, logType)
                        VALUES (?, ?, ?, ?)
                        """,
                        (date_time, nightly_relative, fitbit_user_id, log_type)
                    )
                    insert_count += 1

        conn.commit()
        logging.info(f"Inserted {insert_count} new records and updated {update_count} existing records.")
        logging.info("Skin temperature data insertion completed successfully.")
    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error occurred: {db_err}")
        message = f" :x: ERROR(Insert Skin Temperature): Database error occurred: {db_err}"
        hook.send_text(message)
        raise  # Re-raise the database error to fail the task
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        message = f" :x: ERROR(Insert Skin Temperature): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Re-raise the general error to fail the task
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


# Function to fetch breathing rate data from Fitbit API
def fetch_breathing_rate_data(**kwargs):
    try:
        ti = kwargs['ti']  # Get task instance
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError("No tokens available for API check.")

        all_data = []

        for user_data in tokens:
            fitbit_user_id = user_data.get('fitbit_user_id')
            access_token = user_data.get('access_token')

            if not fitbit_user_id or not access_token:
                logging.error(f"Missing fitbit_user_id or access_token for user {user_data}")
                message = f" :x: ERROR(Fetch Breathing Rate):Missing fitbit_user_id or access_token for user {user_data}"
                hook.send_text(message)
                continue
            else:
                logging.info(f"Fetching breathing rate data for {fitbit_user_id}")

            api_url = f"{FITBIT_BASE_URL}1/user/{fitbit_user_id}{FITBIT_API_ENDPOINT['BREATHING_RATE']}"
            # logging.info(f'API URL: {api_url}')

            headers = {"Authorization": f"Bearer {access_token}"}

            response = requests.get(api_url, headers=headers)
            # logging.info(f"API Response for user {fitbit_user_id}: {response.status_code}")

            data = response.json()
            if response.status_code == 200:
                if 'br' in data:
                    all_data.append({
                        "fitbit_user_id": fitbit_user_id,
                        "br": data['br']
                    })
                else:
                    logging.error(f"No breathing rate data for user {fitbit_user_id}.")
                    message = f" :x: ERROR(Fetch Breathing Rate):No breathing rate data for user {fitbit_user_id}."
                    hook.send_text(message)
                    return None
            else:
                error_type = data.get('errors', [{}])[0].get('errorType', 'Unknown')
                logging.error(
                    f" :x: ERROR fetching data for user {fitbit_user_id}: {response.status_code}, "
                    f" :x: ERROR Type: {error_type}"
                )
                message = f" :x: ERROR(Fetch Breathing Rate):Error fetching data for user {fitbit_user_id}: {response.status_code}, Error Type: {error_type}"
                hook.send_text(message)

        # Check if any data was collected
        if not all_data:
            raise ValueError("No skin temperature data collected for any user.")

            # Push the gathered data to XCom
        ti.xcom_push(key='breathing_rate_data', value=all_data)
        # logging.info(f"Skin temperature data pushed to XCom: {all_data}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        message = f" :x: ERROR(Fetch Breathing Rate):Request exception occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        message = f" :x: ERROR(Fetch Breathing Rate):Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task


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


def fetch_sleep_data(**kwargs):
    try:
        ti = kwargs['ti']  # Get task instance
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError(f"No tokens available for API check.{tokens}")

        # Assume we're calling a real API endpoint to fetch sleep data
        # logging.info('api_url: ' + api_url)
        sleep_data = []

        for user_data in tokens:
            api_url = f"{FITBIT_BASE_URL}1.2/user/{fitbit_user_id}{FITBIT_API_ENDPOINT['SLEEP']}"
            fitbit_user_id = user_data.get('fitbit_user_id')
            logging.info(f"Fetching sleep data for user {fitbit_user_id}")
            access_token = user_data.get('access_token')
            response = requests.get(api_url, headers={"Authorization": f"Bearer {access_token}"})
            data = response.json()
            if not data:
                logging.warning(f"No sleep data found for user {fitbit_user_id}.")
                message = f" :x: ERROR(Fetch Sleep): No sleep data found for user {fitbit_user_id}."
                hook.send_text(message)
                continue
            # logging.info(f'Response(Sleep): {data}')
            if response.status_code == 200:
                sleep_data.append({
                    'fitbit_user_id': fitbit_user_id,
                    'sleep': data['sleep']
                })
            else:
                logging.info(f" :x: ERROR fetching sleep data for {fitbit_user_id}: {response.status_code}")
                message = f" :x: ERROR(Fetch Sleep):Error fetching sleep data for {fitbit_user_id}: {response.status_code}"
                hook.send_text(message)

        kwargs['ti'].xcom_push(key='sleep_data', value=sleep_data)
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        message = f" :x: ERROR(Fetch Sleep):Request exception occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        message = f" :x: ERROR(Fetch Sleep):Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task


# Step 4: Transform sleep data
def transform_sleep_data(**kwargs):
    # Pull sleep data from XCom or from an earlier task
    sleep_data = kwargs['ti'].xcom_pull(key='sleep_data', task_ids='fetch_sleep_data_task')
    # logging.info(f'sleep_data: {sleep_data}')
    transformed_sleep_data = []
    for user_data in sleep_data:
        fitbit_user_id = user_data['fitbit_user_id']
        sleeps = user_data['sleep']
        # logging.info(f'fitbit id: {fitbit_user_id}')
        # logging.info(f'sleeps: {sleeps}')
        # Check if sleeps
        if not sleeps:
            logging.warning(f"No sleep data found for {fitbit_user_id}.")
            message = f":x: ERROR(Transform Sleep): No sleep data found for {fitbit_user_id}."
            hook.send_text(message)
            continue
        logging.info(f'Processing sleep data for {fitbit_user_id}')
        for sleep in sleeps:
            sleep_entry = {}
            sleep_entry['fitbit_user_id'] = fitbit_user_id

            # Extract common sleep data
            sleep_entry['dateOfSleep'] = sleep.get('dateOfSleep')
            sleep_entry['duration'] = sleep.get('duration')
            sleep_entry['efficiency'] = sleep.get('efficiency')
            sleep_entry['endTime'] = sleep.get('endTime')
            sleep_entry['infoCode'] = sleep.get('infoCode')
            sleep_entry['isMainSleep'] = sleep.get('isMainSleep')
            sleep_entry['logId'] = sleep.get('logId')
            sleep_entry['logType'] = sleep.get('logType')
            sleep_entry['minutesAfterWakeup'] = sleep.get('minutesAfterWakeup')
            sleep_entry['minutesAsleep'] = sleep.get('minutesAsleep')
            sleep_entry['minutesAwake'] = sleep.get('minutesAwake')
            sleep_entry['minutesToFallAsleep'] = sleep.get('minutesToFallAsleep')
            sleep_entry['startTime'] = sleep.get('startTime')
            sleep_entry['timeInBed'] = sleep.get('timeInBed')
            sleep_entry['type'] = sleep.get('type')

            # Levels summary for deep, light, rem, and wake sleep
            levels_summary = sleep.get('levels', {}).get('summary', {})

            if sleep_entry['type'] == "stages":
                sleep_entry['deep_sleep_count'] = levels_summary.get('deep', {}).get('count')
                sleep_entry['deep_sleep_minutes'] = levels_summary.get('deep', {}).get('minutes')
                sleep_entry['deep_sleep_30DayAvgMinutes'] = levels_summary.get('deep', {}).get('thirtyDayAvgMinutes')

                sleep_entry['light_sleep_count'] = levels_summary.get('light', {}).get('count')
                sleep_entry['light_sleep_minutes'] = levels_summary.get('light', {}).get('minutes')
                sleep_entry['light_sleep_30DayAvgMinutes'] = levels_summary.get('light', {}).get('thirtyDayAvgMinutes')

                sleep_entry['rem_sleep_count'] = levels_summary.get('rem', {}).get('count')
                sleep_entry['rem_sleep_minutes'] = levels_summary.get('rem', {}).get('minutes')
                sleep_entry['rem_sleep_30DayAvgMinutes'] = levels_summary.get('rem', {}).get('thirtyDayAvgMinutes')

                sleep_entry['wake_sleep_count'] = levels_summary.get('wake', {}).get('count')
                sleep_entry['wake_sleep_minutes'] = levels_summary.get('wake', {}).get('minutes')
                sleep_entry['wake_sleep_30DayAvgMinutes'] = levels_summary.get('wake', {}).get('thirtyDayAvgMinutes')

                # Fill None for classic type fields
                sleep_entry['asleep_sleep_count'] = None
                sleep_entry['asleep_sleep_minutes'] = None
                sleep_entry['awake_sleep_count'] = None
                sleep_entry['awake_sleep_minutes'] = None
                sleep_entry['restless_sleep_count'] = None
                sleep_entry['restless_sleep_minutes'] = None

            elif sleep_entry['type'] == "classic":
                sleep_entry['asleep_sleep_count'] = levels_summary.get('asleep', {}).get('count')
                sleep_entry['asleep_sleep_minutes'] = levels_summary.get('asleep', {}).get('minutes')

                sleep_entry['awake_sleep_count'] = levels_summary.get('awake', {}).get('count')
                sleep_entry['awake_sleep_minutes'] = levels_summary.get('awake', {}).get('minutes')

                sleep_entry['restless_sleep_count'] = levels_summary.get('restless', {}).get('count')
                sleep_entry['restless_sleep_minutes'] = levels_summary.get('restless', {}).get('minutes')

                # Fill None for stages type fields
                sleep_entry['deep_sleep_count'] = None
                sleep_entry['deep_sleep_minutes'] = None
                sleep_entry['deep_sleep_30DayAvgMinutes'] = None
                sleep_entry['light_sleep_count'] = None
                sleep_entry['light_sleep_minutes'] = None
                sleep_entry['light_sleep_30DayAvgMinutes'] = None
                sleep_entry['rem_sleep_count'] = None
                sleep_entry['rem_sleep_minutes'] = None
                sleep_entry['rem_sleep_30DayAvgMinutes'] = None
                sleep_entry['wake_sleep_count'] = None
                sleep_entry['wake_sleep_minutes'] = None
                sleep_entry['wake_sleep_30DayAvgMinutes'] = None

            # logging.info(f'sleep_entry {sleep_entry}')
            transformed_sleep_data.append(sleep_entry)

    # Push transformed data to   XCom for the next task
    kwargs['ti'].xcom_push(key='transformed_sleep_data', value=transformed_sleep_data)
    logging.info("Sleep data pushed to XCom: {transformed_sleep_data}")


# Step 4: Transform sleep data

def transform_sleep_levels_data(**kwargs):
    # Pull sleep data from XCom or from an earlier task
    sleep_data = kwargs['ti'].xcom_pull(key='sleep_data', task_ids='fetch_sleep_data_task')
    # logging.info(f'sleep_data: {sleep_data}')
    transformed_sleep_levels_data = []
    for user_data in sleep_data:
        fitbit_user_id = user_data['fitbit_user_id']
        sleeps = user_data['sleep']
        # logging.info(f'fitbit id: {fitbit_user_id}')

        logging.info(f'Processing sleep levels data for {fitbit_user_id}')
        for sleep in sleeps:

            # Extract common sleep data
            sleep_logid = sleep.get('logId')

            # Levels data for deep, light, rem, and wake sleep in seconds
            sleep_levels_data = sleep.get('levels', {}).get('data', {})

            for sleep_level in sleep_levels_data:
                sleep_level_entry = {}
                # logging.info(f"Processing sleep data of {fitbit_user_id} for {sleep_level['dateTime']} ")
                # logging.info(sleep_level)
                sleep_level_entry['dateTime'] = sleep_level.get('dateTime')
                sleep_level_entry['level'] = sleep_level.get('level')
                sleep_level_entry['seconds'] = sleep_level.get('seconds')
                sleep_level_entry['fitbit_user_id'] = fitbit_user_id
                sleep_level_entry['logId'] = sleep_logid
                # logging.info(sleep_level_entry)
                transformed_sleep_levels_data.append(sleep_level_entry)

    # Push transformed data to XCom for the next task
    kwargs['ti'].xcom_push(key='transformed_sleep_level_data', value=transformed_sleep_levels_data)
    logging.info("Sleep data pushed to XCom: {transformed_sleep_levels_data}")


def insert_sleep_data(**kwargs):
    """
    Inserts Fitbit sleep data into the database, checks for duplicates, and handles errors.

    :param kwargs: Additional keyword arguments provided by Airflow
    """
    try:
        # Pull the transformed sleep data from XCom
        sleep_data = kwargs['ti'].xcom_pull(task_ids='transform_sleep_data_task', key='transformed_sleep_data')

        if not sleep_data:
            raise ValueError("No sleep data available to insert into the database.")

        # Database connection setup
        conn = get_sql_connection()
        cursor = conn.cursor()
        insert_count = 0
        update_count = 0
        for entry in sleep_data:
            fitbit_user_id = entry['fitbit_user_id']
            date_of_sleep = entry['dateOfSleep']
            log_id = entry['logId']

            # Check for duplicate entry (unique constraint check)
            cursor.execute(
                """SELECT COUNT(*)
                   FROM fitbit_sleep_data
                   WHERE dateOfSleep = ?
                     AND fitbit_user_id = ?
                     AND logId = ?""",
                (date_of_sleep, fitbit_user_id, log_id)
            )
            if cursor.fetchone()[0] > 0:
                logging.info(f"Duplicate found, updating sleep data for {fitbit_user_id} on {date_of_sleep}.")

                cursor.execute(
                    """UPDATE fitbit_sleep_data
                       SET duration                    = ?,
                           efficiency                  = ?,
                           endTime                     = ?,
                           infoCode                    = ?,
                           isMainSleep                 = ?,
                           logType                     = ?,
                           minutesAfterWakeup          = ?,
                           minutesAsleep               = ?,
                           minutesAwake                = ?,
                           minutesToFallAsleep         = ?,
                           startTime                   = ?,
                           timeInBed                   = ?,
                           type                        = ?,
                           deep_sleep_count            = ?,
                           deep_sleep_minutes          = ?,
                           deep_sleep_30DayAvgMinutes  = ?,
                           light_sleep_count           = ?,
                           light_sleep_minutes         = ?,
                           light_sleep_30DayAvgMinutes = ?,
                           rem_sleep_count             = ?,
                           rem_sleep_minutes           = ?,
                           rem_sleep_30DayAvgMinutes   = ?,
                           wake_sleep_count            = ?,
                           wake_sleep_minutes          = ?,
                           wake_sleep_30DayAvgMinutes  = ?,
                           asleep_sleep_count          = ?,
                           asleep_sleep_minutes        = ?,
                           awake_sleep_count           = ?,
                           awake_sleep_minutes         = ?,
                           restless_sleep_count        = ?,
                           restless_sleep_minutes      = ?
                       WHERE dateOfSleep = ?
                         AND fitbit_user_id = ?
                         AND logId = ?""",
                    (
                        entry['duration'], entry['efficiency'], entry['endTime'], entry['infoCode'],
                        entry['isMainSleep'], entry['logType'], entry['minutesAfterWakeup'], entry['minutesAsleep'],
                        entry['minutesAwake'], entry['minutesToFallAsleep'], entry['startTime'], entry['timeInBed'],
                        entry['type'], entry['deep_sleep_count'], entry['deep_sleep_minutes'],
                        entry['deep_sleep_30DayAvgMinutes'],
                        entry['light_sleep_count'], entry['light_sleep_minutes'], entry['light_sleep_30DayAvgMinutes'],
                        entry['rem_sleep_count'], entry['rem_sleep_minutes'], entry['rem_sleep_30DayAvgMinutes'],
                        entry['wake_sleep_count'], entry['wake_sleep_minutes'], entry['wake_sleep_30DayAvgMinutes'],
                        entry.get('asleep_sleep_count'), entry.get('asleep_sleep_minutes'),
                        entry.get('awake_sleep_count'), entry.get('awake_sleep_minutes'),
                        entry.get('restless_sleep_count'), entry.get('restless_sleep_minutes'),
                        date_of_sleep, fitbit_user_id, log_id
                    )
                )
                update_count += 1
            else:

                # Insert new data
                logging.info(f"Inserting sleep data for {fitbit_user_id} on {date_of_sleep}.")

                cursor.execute(
                    """INSERT INTO fitbit_sleep_data (fitbit_user_id, dateOfSleep, duration, efficiency, endTime,
                                                      infoCode,
                                                      isMainSleep, logId, logType, minutesAfterWakeup, minutesAsleep,
                                                      minutesAwake, minutesToFallAsleep, startTime, timeInBed, type,
                                                      deep_sleep_count, deep_sleep_minutes, deep_sleep_30DayAvgMinutes,
                                                      light_sleep_count, light_sleep_minutes,
                                                      light_sleep_30DayAvgMinutes,
                                                      rem_sleep_count, rem_sleep_minutes, rem_sleep_30DayAvgMinutes,
                                                      wake_sleep_count, wake_sleep_minutes, wake_sleep_30DayAvgMinutes,
                                                      asleep_sleep_count, asleep_sleep_minutes,
                                                      awake_sleep_count, awake_sleep_minutes,
                                                      restless_sleep_count, restless_sleep_minutes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                               ?, ?, ?, ?)""",
                    (
                        entry['fitbit_user_id'], entry['dateOfSleep'], entry['duration'], entry['efficiency'],
                        entry['endTime'], entry['infoCode'], entry['isMainSleep'], entry['logId'], entry['logType'],
                        entry['minutesAfterWakeup'], entry['minutesAsleep'], entry['minutesAwake'],
                        entry['minutesToFallAsleep'],
                        entry['startTime'], entry['timeInBed'], entry['type'], entry['deep_sleep_count'],
                        entry['deep_sleep_minutes'],
                        entry['deep_sleep_30DayAvgMinutes'], entry['light_sleep_count'], entry['light_sleep_minutes'],
                        entry['light_sleep_30DayAvgMinutes'], entry['rem_sleep_count'], entry['rem_sleep_minutes'],
                        entry['rem_sleep_30DayAvgMinutes'], entry['wake_sleep_count'], entry['wake_sleep_minutes'],
                        entry['wake_sleep_30DayAvgMinutes'],
                        entry.get('asleep_sleep_count'), entry.get('asleep_sleep_minutes'),
                        entry.get('awake_sleep_count'), entry.get('awake_sleep_minutes'),
                        entry.get('restless_sleep_count'), entry.get('restless_sleep_minutes')
                    )
                )
                insert_count += 1

        # Commit the transaction
        conn.commit()
        logging.info(f"{insert_count} sleep records successfully inserted into the database.")
        logging.info(f"{update_count} duplicate records updated.")

    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error occurred: {db_err}")
        message = f" :x: ERROR(Insert Sleep Data): Database error occurred: {db_err}"
        hook.send_text(message)
        raise  # Re-raise the database error to fail the task
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        message = f" :x: ERROR(Insert Sleep): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Re-raise the general error to fail the task
    finally:
        # Close the database connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


def insert_sleep_levels_data(**kwargs):
    """
    Inserts Fitbit sleep levels data into the database, checks for duplicates, and handles errors.

    :param kwargs: Additional keyword arguments provided by Airflow
    """
    try:
        # Pull the transformed sleep levels data from XCom
        sleep_levels_data = kwargs['ti'].xcom_pull(task_ids='transform_sleep_levels_data_task',
                                                   key='transformed_sleep_level_data')

        if not sleep_levels_data:
            raise ValueError("No sleep levels data available to insert into the database.")

        # Database connection setup
        conn = get_sql_connection()
        cursor = conn.cursor()
        insert_count = 0
        update_count = 0
        for entry in sleep_levels_data:
            fitbit_user_id = entry['fitbit_user_id']
            date_time = entry['dateTime']
            sleep_level = entry['level']
            seconds = entry['seconds']
            log_id = entry['logId']

            # Check for duplicate entry (unique constraint check)
            cursor.execute(
                """SELECT COUNT(*)
                   FROM fitbit_sleep_levels_data
                   WHERE dateTime = ?
                     AND fitbit_user_id = ?
                     AND logId = ?""",
                (date_time, fitbit_user_id, log_id)
            )
            if cursor.fetchone()[0] > 0:
                logging.info(
                    f"Duplicate data detected for {fitbit_user_id} on {date_time} with logId {log_id}. Updating record.")
                cursor.execute(
                    """UPDATE fitbit_sleep_levels_data
                       SET level   = ?,
                           seconds = ?
                       WHERE dateTime = ?
                         AND fitbit_user_id = ?
                         AND logId = ?""",
                    (sleep_level, seconds, date_time, fitbit_user_id, log_id)
                )
                update_count += 1
            else:
                # Insert new data
                logging.info(f"Inserting sleep level data for {fitbit_user_id} on {date_time} with logId {log_id}.")

                cursor.execute(
                    """INSERT INTO fitbit_sleep_levels_data (dateTime, level, seconds, fitbit_user_id, logId)
                       VALUES (?, ?, ?, ?, ?)""",
                    (date_time, sleep_level, seconds, fitbit_user_id, log_id)
                )
                insert_count += 1

        # Commit the transaction
        conn.commit()
        logging.info(f"{insert_count} sleep level records successfully inserted into the database.")
        logging.info(f"{update_count} duplicate records updated.")

    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error occurred: {db_err}")
        message = f" :x: ERROR(Insert Sleep Levels): Database error occurred: {db_err}"
        hook.send_text(message)
        raise  # Re-raise the database error to fail the task
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        message = f" :x: ERROR(Insert Sleep Levels): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Re-raise the general error to fail the task
    finally:
        # Close the database connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


# Step 3: Fetch cardio_score data
def fetch_cardio_score_data(**kwargs):
    try:
        ti = kwargs['ti']  # Get task instance
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError(f"No tokens available for API check.{tokens}")

        # Assume we're calling a real API endpoint to fetch cardio_score data
        #
        cardio_score_data = []

        for user_data in tokens:
            fitbit_user_id = user_data.get('fitbit_user_id')
            access_token = user_data.get('access_token')
            api_url = f"{FITBIT_BASE_URL}1/user/{fitbit_user_id}{FITBIT_API_ENDPOINT['CARDIO_SCORE']}"
            logging.info('api_url: ' + api_url)
            response = requests.get(api_url, headers={"Authorization": f"Bearer {access_token}"})
            data = response.json()
            logging.info(f'Fetching cardioScore for {fitbit_user_id}')
            if response.status_code == 200:
                cardio_score_data.append({
                    'fitbit_user_id': fitbit_user_id,
                    'cardio_score': data['cardioScore']
                })
            else:
                logging.info(f" :x: ERROR fetching cardio_score data for {fitbit_user_id}: {response.status_code}")

        kwargs['ti'].xcom_push(key='cardio_score_data', value=cardio_score_data)
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        message = f" :x: ERROR(Fetch Cardio Score):Request exception occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        message = f" :x: ERROR(Fetch Cardio Score):Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task


def insert_cardio_score_data(**kwargs):
    """
    Inserts Fitbit cardio_score data into the database, checks for duplicates, and handles errors.

    :param kwargs: Additional keyword arguments provided by Airflow
    """
    try:
        # Pull the transformed cardio_score data from XCom
        cardio_score_data = kwargs['ti'].xcom_pull(task_ids='fetch_cardio_score_data_task',
                                                   key='cardio_score_data')

        if not cardio_score_data:
            raise ValueError("No cardio_score data available to insert into the database.")

        # Database connection setup
        conn = get_sql_connection()
        cursor = conn.cursor()
        insert_count = 0
        update_count = 0
        for user_data in cardio_score_data:
            fitbit_user_id = user_data['fitbit_user_id']
            cardio_score_records = user_data['cardio_score']

            for entry in cardio_score_records:
                date_of_cardio_score = entry['dateTime']
                vo2_max = entry['value']['vo2Max']

                # Check for duplicate entry (unique constraint check)
                cursor.execute(
                    """SELECT COUNT(*)
                       FROM fitbit_cardio_score
                       WHERE date_time = ?
                         AND fitbit_user_id = ? """,
                    (date_of_cardio_score, fitbit_user_id)
                )
                if cursor.fetchone()[0] > 0:
                    # Record exists - perform update
                    logging.info(f"Updating cardio_score data for {fitbit_user_id} on {date_of_cardio_score}.")
                    update_query = """
                                   UPDATE [dbo].[fitbit_cardio_score]
                                   SET vo2_max = ?
                                   WHERE date_time = ?
                                     AND fitbit_user_id = ? \
                                   """
                    cursor.execute(update_query, (vo2_max, date_of_cardio_score, fitbit_user_id))
                    update_count += 1
                else:
                    # Insert new data
                    logging.info(f"Inserting cardio_score data for {fitbit_user_id} on {date_of_cardio_score}.")

                    # Insert query
                    insert_query = """
                                   INSERT INTO [dbo].[fitbit_cardio_score]
                                       ([date_time], [vo2_max], [fitbit_user_id])
                                   VALUES (?, ?, ?) \
                                   """
                    cursor.execute(insert_query, (date_of_cardio_score, vo2_max, fitbit_user_id))

                    insert_count += 1

        # Commit the transaction
        conn.commit()
        logging.info(f"{insert_count} cardio_score records successfully inserted into the database.")
        logging.info(f"{update_count} duplicate records skipped.")

    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error occurred: {db_err}")
        message = f" :x: ERROR(Insert Cardio Score): Database error occurred: {db_err}"
        hook.send_text(message)
        raise  # Re-raise the database error to fail the task
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        message = f" :x: ERROR(Insert Cardio Score): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Re-raise the general error to fail the task
    finally:
        # Close the database connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


# Step 3: Fetch heart_rate_variability data
def fetch_heart_rate_variability_data(**kwargs):
    try:
        ti = kwargs['ti']  # Get task instance
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError(f"No tokens available for API check.{tokens}")

        # Assume we're calling a real API endpoint to fetch heart_rate_variability data

        heart_rate_variability_data = []

        for user_data in tokens:
            fitbit_user_id = user_data.get('fitbit_user_id')
            access_token = user_data.get('access_token')
            logging.info(f"Fetching heart_rate_variability data for {fitbit_user_id}")
            api_url = f"{FITBIT_BASE_URL}1/user/{fitbit_user_id}{FITBIT_API_ENDPOINT['HEART_RATE_VARIABILITY']}"
            logging.info('api_url: ' + api_url)
            response = requests.get(api_url, headers={"Authorization": f"Bearer {access_token}"})
            data = response.json()

            # logging.info(f'Response(HRV): {data}')
            if response.status_code == 200:
                if data.get('hrv'):  # This checks if 'hrv' exists and is not empty
                    heart_rate_variability_data.append({
                        'fitbit_user_id': fitbit_user_id,
                        'heart_rate_variability': data['hrv']
                    })
                else:
                    logging.warning(f"No heart_rate_variability data for {fitbit_user_id}")
            else:
                logging.info(
                    f" :x: ERROR fetching heart_rate_variability data for {fitbit_user_id}: {response.status_code}")

        kwargs['ti'].xcom_push(key='heart_rate_variability_data', value=heart_rate_variability_data)
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        message = f" :x: ERROR(Fetch Heart Rate Variability):Request exception occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        message = f" :x: ERROR(Fetch Heart Rate Variability): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task


def insert_heart_rate_variability_data(**kwargs):
    """
    Inserts Fitbit heart_rate_variability data into the database, checks for duplicates, and handles errors.

    :param kwargs: Additional keyword arguments provided by Airflow
    """
    try:
        # Pull the transformed heart_rate_variability data from XCom
        heart_rate_variability_data = kwargs['ti'].xcom_pull(task_ids='fetch_heart_rate_variability_data_task',
                                                             key='heart_rate_variability_data')

        if not heart_rate_variability_data:
            raise ValueError("No heart_rate_variability data available to insert into the database.")

        # Database connection setup
        conn = get_sql_connection()
        cursor = conn.cursor()
        insert_count = 0
        update_count = 0
        for user_data in heart_rate_variability_data:
            fitbit_user_id = user_data['fitbit_user_id']
            heart_rate_variability_records = user_data['heart_rate_variability']
            logging.info(f'Inserting HRV data for id: {fitbit_user_id}')
            for entry in heart_rate_variability_records:
                date_time = entry['dateTime']
                daily_rmssd = entry['value']['dailyRmssd']
                deep_rmssd = entry['value']['deepRmssd']

                # Check for duplicate entry (unique constraint check)
                cursor.execute(
                    """SELECT COUNT(*)
                       FROM fitbit_heart_rate_variability_data
                       WHERE date_time = ?
                         AND fitbit_user_id = ? """,
                    (date_time, fitbit_user_id)
                )
                if cursor.fetchone()[0] > 0:
                    # Update existing record
                    logging.info(f"Updating heart_rate_variability data for {fitbit_user_id} on {date_time}.")
                    update_query = """
                                   UPDATE [dbo].[fitbit_heart_rate_variability_data]
                                   SET daily_rmssd = ?,
                                       deep_rmssd  = ?
                                   WHERE date_time = ?
                                     AND fitbit_user_id = ? \
                                   """
                    cursor.execute(update_query, (daily_rmssd, deep_rmssd, date_time, fitbit_user_id))
                    update_count += 1
                else:
                    # Insert new data
                    logging.info(f"Inserting heart_rate_variability data for {fitbit_user_id} on {date_time}.")

                    # Insert data into the table
                    insert_query = f"""
                            INSERT INTO [dbo].[fitbit_heart_rate_variability_data]
                               ([date_time]
                               ,[daily_rmssd]
                               ,[deep_rmssd]
                               ,[fitbit_user_id])
                             VALUES
                               ('{date_time}', {daily_rmssd}, {deep_rmssd}, '{fitbit_user_id}')
                            """

                    cursor.execute(insert_query)

                    insert_count += 1

        # Commit the transaction
        conn.commit()
        logging.info(f"{insert_count} heart_rate_variability records successfully inserted into the database.")
        logging.info(f"{update_count} duplicate records skipped.")

    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error occurred: {db_err}")
        message = f" :x: ERROR(Insert Heart Rate Variability): Database error occurred: {db_err}"
        hook.send_text(message)
        raise  # Re-raise the database error to fail the task
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        message = f" :x: ERROR(Insert Heart Rate Variability): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Re-raise the general error to fail the task
    finally:
        # Close the database connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


# Step 3: Fetch heart_rate data
def fetch_heart_rate_data(**kwargs):
    try:
        ti = kwargs['ti']  # Get task instance
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError(f"No tokens available for API check.{tokens}")

        heart_rate_data = []

        for user_data in tokens:
            fitbit_user_id = user_data.get('fitbit_user_id')
            access_token = user_data.get('access_token')
            logging.info(
                f'Fetching Heart Rate Data for {fitbit_user_id}')  # Assume we're calling a real API endpoint to fetch heart_rate data
            api_url = f"{FITBIT_BASE_URL}1/user/{fitbit_user_id}{FITBIT_API_ENDPOINT['HEART_RATE']}"

            logging.info('api_url: ' + api_url)
            response = requests.get(api_url, headers={"Authorization": f"Bearer {access_token}"})
            data = response.json()
            # logging.info(f'Response(cardioScore): {data}')
            if response.status_code == 200:
                heart_rate_data.append({
                    'fitbit_user_id': fitbit_user_id,
                    'activities_heart': data['activities-heart']
                })
            else:
                print(f" :x: ERROR fetching heart_rate data for {fitbit_user_id}: {response.status_code}")

        kwargs['ti'].xcom_push(key='heart_rate_data', value=heart_rate_data)
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        message = f" :x: ERROR (Fetch Heart Rate): Request exception occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        message = f" :x: ERROR (Fetch Heart Rate): Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task


def transform_heart_rate_data(**kwargs):
    # Pull heart_rate data from XCom or from an earlier task
    heart_rate_data = kwargs['ti'].xcom_pull(key='heart_rate_data', task_ids='fetch_heart_rate_data_task')
    #   logging.info(f'heart_rate_data: {heart_rate_data}')
    transformed_heart_rate_data = []
    for user_data in heart_rate_data:
        fitbit_user_id = user_data['fitbit_user_id']
        activities_heart = user_data['activities_heart']
        logging.info(f'Processing heart rate data for id: {fitbit_user_id}')
        # logging.info(f'heart_rates: {activities_heart}')

        for heart_rate in activities_heart:
            heart_rate_entry = {}
            heart_rate_entry['fitbit_user_id'] = fitbit_user_id

            date_time = heart_rate['dateTime']
            resting_heart_rate = heart_rate['value']['restingHeartRate'] if 'restingHeartRate' in heart_rate[
                'value'] else None
            zones = heart_rate['value']['heartRateZones']

            # Initialize variables for heart rate zones
            caloriesOutOfRange = minutesOutOfRange = 0
            caloriesFatBurn = minutesFatBurn = 0
            caloriesCardio = minutesCardio = 0
            caloriesPeak = minutesPeak = 0

            # Loop through heart rate zones to extract data
            for zone in zones:
                if 'caloriesOut' in zone or 'minutes' in zone:
                    if zone['name'] == 'Out of Range':
                        caloriesOutOfRange = zone['caloriesOut']
                        minutesOutOfRange = zone['minutes']
                    elif zone['name'] == 'Fat Burn':
                        caloriesFatBurn = zone['caloriesOut']
                        minutesFatBurn = zone['minutes']
                    elif zone['name'] == 'Cardio':
                        caloriesCardio = zone['caloriesOut']
                        minutesCardio = zone['minutes']
                    elif zone['name'] == 'Peak':
                        caloriesPeak = zone['caloriesOut']
                        minutesPeak = zone['minutes']

            heart_rate_entry['date_time'] = date_time
            heart_rate_entry['fitbit_user_id'] = fitbit_user_id
            heart_rate_entry['resting_heart_rate'] = resting_heart_rate
            heart_rate_entry['calories_out_of_range'] = caloriesOutOfRange
            heart_rate_entry['minutes_out_of_range'] = minutesOutOfRange
            heart_rate_entry['calories_fat_burn'] = caloriesFatBurn
            heart_rate_entry['minutes_fat_burn'] = minutesFatBurn
            heart_rate_entry['calories_cardio'] = caloriesCardio
            heart_rate_entry['minutes_cardio'] = minutesCardio
            heart_rate_entry['calories_peak'] = caloriesPeak
            heart_rate_entry['minutes_peak'] = minutesPeak

            # logging.info(f'heart_rate_entry {heart_rate_entry}')
            transformed_heart_rate_data.append(heart_rate_entry)

    # Push transformed data to   XCom for the next task
    kwargs['ti'].xcom_push(key='transformed_heart_rate_data', value=transformed_heart_rate_data)
    logging.info("heart_rate data pushed to XCom: {transformed_heart_rate_data}")


def insert_heart_rate_data(**kwargs):
    """
    Inserts Fitbit heart_rate data into the database, checks for duplicates, and handles errors.

    :param kwargs: Additional keyword arguments provided by Airflow
    """
    try:
        # Pull the transformed heart_rate data from XCom
        heart_rate_data = kwargs['ti'].xcom_pull(task_ids='transform_heart_rate_data_task',
                                                 key='transformed_heart_rate_data')

        if not heart_rate_data:
            raise ValueError("No heart_rate data available to insert into the database.")

        # Database connection setup
        conn = get_sql_connection()

        #     SELECT
        #     , [date_time]
        #     , [resting_heart_rate]
        #     , [calories_out_of_range]
        #     , [minutes_out_of_range]
        #     , [calories_fat_burn]
        #     , [minutes_fat_burn]
        #     , [calories_cardio]
        #     , [minutes_cardio]
        #     , [calories_peak]
        #     , [minutes_peak]
        #     , [fitbit_user_id]
        #       FROM[heaps - pilot].[dbo].[fitbit_heart_rate_data]

        cursor = conn.cursor()
        insert_count = 0
        duplicate_count = 0
        update_count = 0

        for user_data in heart_rate_data:
            print(json.dumps(user_data, indent=4))
            logging.info(f'user_data: {user_data}')

            fitbit_user_id = user_data['fitbit_user_id']
            logging.info(f'Inserting heart rate data for id: {fitbit_user_id}')

            date_time = user_data['date_time']
            logging.info(
                f"fitbit_user_id: {fitbit_user_id} date_time: {date_time} resting_heart_rate: {user_data['resting_heart_rate']}")
            resting_heart_rate = user_data['resting_heart_rate'] if 'resting_heart_rate' in user_data else None
            calories_out_of_range = user_data['calories_out_of_range'] if 'calories_out_of_range' in user_data else None
            minutes_out_of_range = user_data['minutes_out_of_range'] if 'minutes_out_of_range' in user_data else None
            calories_fat_burn = user_data['calories_fat_burn'] if 'calories_fat_burn' in user_data else None
            minutes_fat_burn = user_data['minutes_fat_burn'] if 'minutes_fat_burn' in user_data else None
            calories_cardio = user_data['calories_cardio'] if 'calories_cardio' in user_data else None
            minutes_cardio = user_data['minutes_cardio'] if 'minutes_cardio' in user_data else None
            calories_peak = user_data['calories_peak'] if 'calories_peak' in user_data else None
            minutes_peak = user_data['minutes_peak'] if 'minutes_peak' in user_data else None

            # Check for duplicate entry (unique constraint check)
            cursor.execute(
                """SELECT COUNT(*)
                   FROM fitbit_heart_rate_data
                   WHERE date_time = ?
                     AND fitbit_user_id = ? """,
                (date_time, fitbit_user_id)
            )

            # Insert new data
            # logging.info(f"Inserting heart_rate data for {fitbit_user_id} on {date_time}.")

            # Check if data already exists in the database
            check_query = """
                          SELECT 1
                          FROM fitbit_heart_rate_data
                          WHERE date_time = ?
                            AND fitbit_user_id = ? \
                          """
            cursor.execute(check_query, (date_time, fitbit_user_id))
            if cursor.fetchone():
                # Update existing record
                update_query = """
                               UPDATE fitbit_heart_rate_data
                               SET resting_heart_rate    = ?,
                                   calories_out_of_range = ?,
                                   minutes_out_of_range  = ?,
                                   calories_fat_burn     = ?,
                                   minutes_fat_burn      = ?,
                                   calories_cardio       = ?,
                                   minutes_cardio        = ?,
                                   calories_peak         = ?,
                                   minutes_peak          = ?
                               WHERE date_time = ?
                                 AND fitbit_user_id = ? \
                               """
                cursor.execute(update_query, (
                    resting_heart_rate,
                    calories_out_of_range, minutes_out_of_range,
                    calories_fat_burn, minutes_fat_burn,
                    calories_cardio, minutes_cardio,
                    calories_peak, minutes_peak,
                    date_time, fitbit_user_id
                ))
                conn.commit()
                logging.info(f"Data updated successfully for {fitbit_user_id} at {date_time}.")
                update_count += 1
            else:
                # Insert data into the database
                insert_query = """
                               INSERT INTO fitbit_heart_rate_data (date_time, resting_heart_rate,
                                                                   calories_out_of_range, minutes_out_of_range,
                                                                   calories_fat_burn, minutes_fat_burn,
                                                                   calories_cardio, minutes_cardio,
                                                                   calories_peak, minutes_peak,
                                                                   fitbit_user_id)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                               """
                cursor.execute(insert_query, (
                    date_time, resting_heart_rate,
                    calories_out_of_range, minutes_out_of_range,
                    calories_fat_burn, minutes_fat_burn,
                    calories_cardio, minutes_cardio,
                    calories_peak, minutes_peak,
                    fitbit_user_id
                ))
                conn.commit()
                logging.info(f"Data inserted successfully for {fitbit_user_id} at {date_time}.")
                insert_count += 1

        # Commit the transaction
        conn.commit()
        logging.info(f"{insert_count} heart_rate records successfully inserted into the database.")
        logging.info(f"{update_count} records updated.")

    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error occurred: {db_err}")
        message = f":x: Error(Insert Heart Rate):Database error occurred: {db_err}"
        hook.send_text(message)
        raise  # Re-raise the database error to fail the task
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        message = f":x: Error(Insert Heart Rate):An unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Re-raise the general error to fail the task
    finally:
        # Close the database connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


# Define the function to fetch Fitbit activity data
def fetch_activity_data(**kwargs):
    try:
        ti = kwargs['ti']  # Get task instance
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError(f"No tokens available for API check.{tokens}")

        activity_apis = {
            'distance': 'activities-distance',
            'minutesSedentary': 'activities-minutesSedentary',
            'minutesLightlyActive': 'activities-minutesLightlyActive',
            'minutesFairlyActive': 'activities-minutesFairlyActive',
            'minutesVeryActive': 'activities-minutesVeryActive',
            'activityCalories': 'activities-activityCalories'
        }

        activities_data = []

        for activity_key, api_object in activity_apis.items():
            logging.info(f"Fetching {activity_key}")

            # logging.info('api_url: ' + api_url)

            for user_data in tokens:
                try:
                    fitbit_user_id = user_data.get('fitbit_user_id')
                    logging.info(f"-- for {fitbit_user_id}")
                    access_token = user_data.get('access_token')
                    api_url = f"{FITBIT_BASE_URL}1/user/{fitbit_user_id}{FITBIT_API_ENDPOINT['ACTIVITIES']}"
                    response = requests.get(api_url, headers={"Authorization": f"Bearer {access_token}"})
                    data = response.json()
                    # logging.info(f'Response({api_object}): {data}')
                    if response.status_code == 200:
                        # Store raw activity data in activities_data
                        activities_data.append({
                            'fitbit_user_id': fitbit_user_id,
                            'activity_key': activity_key,
                            'api_object': api_object,
                            'data': data.get(api_object, [])
                        })
                    else:
                        logging.error(
                            f" :x: ERROR fetching activities data for {fitbit_user_id}: {response.status_code}")
                except Exception as e:
                    logging.error(f"Error fetching data for user {user_data.get('fitbit_user_id')}: {e}")
                    message = f" :x: ERROR(Fetch Activities):Error fetching data for user {user_data.get('fitbit_user_id')}: {e}"
                    hook.send_text(message)
                    continue

        # Pass fetched data to the next task for transformation
        kwargs['ti'].xcom_push(key='activities_data', value=activities_data)

    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        message = f":x: Error(Fetch Activities):Request exception occurred: {e}"
        hook.send_text(message)

        raise  # Reraise to fail the task
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        message = f":x: Error(Fetch Activities):Unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Reraise to fail the task


def transform_activities_data(**kwargs):
    """
    Transforms raw Fitbit activity data into a combined format per user and date.
    Each record will include all activity types for a given user and date.
    """
    try:
        activities_data = kwargs['ti'].xcom_pull(
            key='activities_data', task_ids='fetch_activity_data_task'
        )

        if not activities_data:
            raise ValueError("No data to transform.")

        combined_data = {}

        for activity in activities_data:
            activity_key = activity['activity_key']
            fitbit_user_id = activity['fitbit_user_id']
            data = activity['data']

            for entry in data:
                date_time = entry.get('dateTime')
                value = entry.get('value')

                # Unique key per user per day
                user_day_key = (fitbit_user_id, date_time)

                if user_day_key not in combined_data:
                    combined_data[user_day_key] = {
                        'fitbit_user_id': fitbit_user_id,
                        'dateTime': date_time
                    }

                # Add or update the activity type value
                combined_data[user_day_key][activity_key] = value

        # Final list of transformed records
        transformed_data = list(combined_data.values())

        # Optionally: Push to next task
        kwargs['ti'].xcom_push(key='combined_activities_data', value=transformed_data)

        return transformed_data

    except Exception as e:
        raise RuntimeError(f"Error in transform_activities_data: {e}")


# Define the function to insert the combined data into the database
def insert_combined_activities_data(**kwargs):
    """
    INSERT INTO [dbo].[fitbit_activity_time_series]
           ([fitbit_user_id]
           ,[dateTime]
           ,[distance]
           ,[minutesSedentary]
           ,[minutesLightlyActive]
           ,[minutesFairlyActive]
           ,[minutesVeryActive]
           ,[activityCalories])
     VALUES
           (<fitbit_user_id, nvarchar(10),>
           ,<dateTime, date,>
           ,<distance, float,>
           ,<minutesSedentary, int,>
           ,<minutesLightlyActive, int,>
           ,<minutesFairlyActive, int,>
           ,<minutesVeryActive, int,>
           ,<activityCalories, int,>)
    """
    try:
        combined_data = kwargs['ti'].xcom_pull(key='combined_activities_data',
                                               task_ids='transform_activities_data_task')

        if not combined_data:
            raise ValueError("No data to insert.")
        # logging.info(combined_data)
        # Database connection setup
        conn = get_sql_connection()
        cursor = conn.cursor()
        insert_count = 0
        update_count = 0

        # Loop through each date entry in combined_data
        for data in combined_data:

            fitbit_user_id = data['fitbit_user_id']
            date_time = data['dateTime']
            distance = data.get('distance')
            minutes_sedentary = data.get('minutesSedentary')
            minutes_lightly_active = data.get('minutesLightlyActive')
            minutes_fairly_active = data.get('minutesFairlyActive')
            minutes_very_active = data.get('minutesVeryActive')
            activity_calories = data.get('activityCalories')

            # Check if data already exists in the database
            check_query = """
                          SELECT 1
                          FROM fitbit_activity_time_series
                          WHERE datetime = ?
                            AND fitbit_user_id = ? \
                          """
            cursor.execute(check_query, (date_time, fitbit_user_id))
            # logging.info(f"{check_query} {date_time} {fitbit_user_id}")
            if cursor.fetchone():
                update_query = """
                               UPDATE fitbit_activity_time_series
                               SET distance             = ?,
                                   minutesSedentary     = ?,
                                   minutesLightlyActive = ?,
                                   minutesFairlyActive  = ?,
                                   minutesVeryActive    = ?,
                                   activityCalories     = ?
                               WHERE dateTime = ?
                                 AND fitbit_user_id = ? \
                               """
                cursor.execute(
                    update_query,
                    (distance, minutes_sedentary, minutes_lightly_active, minutes_fairly_active,
                     minutes_very_active, activity_calories, date_time, fitbit_user_id)
                )
                update_count += 1
                logging.info(f"Updated data for {fitbit_user_id} at {date_time}.")
            else:
                # Insert data into the database
                insert_query = """
                               INSERT INTO fitbit_activity_time_series (fitbit_user_id, dateTime, distance,
                                                                        minutesSedentary,
                                                                        minutesLightlyActive, minutesFairlyActive,
                                                                        minutesVeryActive, activityCalories)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                               """
                cursor.execute(
                    insert_query,
                    (fitbit_user_id, date_time, distance, minutes_sedentary, minutes_lightly_active,
                     minutes_fairly_active,
                     minutes_very_active, activity_calories)
                )
                insert_count += 1

        conn.commit()
        logging.info(f"{insert_count} records successfully inserted into the database.")
        logging.info(f"{update_count} duplicate records skipped.")

    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error occurred: {db_err}")
        message = f":x: Error(Insert fitbit_activity_time_series): Database error occurred: {db_err}"
        hook.send_text(message)

        raise  # Re-raise the database error to fail the task
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        message = f":x: Error(Insert fitbit_activity_time_series): An unexpected error occurred: {e}"
        hook.send_text(message)
        raise  # Re-raise the general error to fail the task
    finally:
        # Close the database connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


# Define the DAG
dag = DAG(
    'fitbit_health_data_pipeline_optimised',
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
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
    dag=dag
)

# Airflow task to fetch breathing rate data for each user
fetch_breathing_rate_data_task = PythonOperator(
    task_id='fetch_breathing_rate_data',
    python_callable=fetch_breathing_rate_data,
    provide_context=True,  # Ensure context is passed to your function
    dag=dag,
)

# Airflow task to fetch skin temperature data for each user
fetch_skin_temperature_data_task = PythonOperator(
    task_id='fetch_skin_temperature_data',
    python_callable=fetch_skin_temperature_data,
    provide_context=True,
    dag=dag
)

# Airflow task to insert skin temperature data
insert_skin_temperature_data_task = PythonOperator(
    task_id='insert_skin_temperature_data',
    python_callable=insert_skin_temperature_data,
    provide_context=True,
    dag=dag
)

# Airflow task to insert breathing rate data
insert_breathing_rate_data_task = PythonOperator(
    task_id='insert_breathing_rate_data',
    python_callable=insert_breathing_rate_data,
    dag=dag
)

fetch_sleep_data_task = PythonOperator(
    task_id='fetch_sleep_data_task',
    python_callable=fetch_sleep_data,
    provide_context=True,
    dag=dag,
)

transform_sleep_data_task = PythonOperator(
    task_id='transform_sleep_data_task',
    python_callable=transform_sleep_data,
    provide_context=True,
    dag=dag,
)
transform_sleep_levels_data_task = PythonOperator(
    task_id='transform_sleep_levels_data_task',
    python_callable=transform_sleep_levels_data,
    provide_context=True,
    dag=dag,
)

insert_sleep_data_task = PythonOperator(
    task_id='load_insert_sleep_data_task',
    python_callable=insert_sleep_data,
    provide_context=True,
    dag=dag,
)

insert_sleep_levels_data_task = PythonOperator(
    task_id='load_insert_sleep_levels_data_task',
    python_callable=insert_sleep_levels_data,
    provide_context=True,
    dag=dag,
)

fetch_cardio_score_data_task = PythonOperator(
    task_id='fetch_cardio_score_data_task',
    python_callable=fetch_cardio_score_data,
    provide_context=True,
    dag=dag,
)

insert_cardio_score_data_task = PythonOperator(
    task_id='load_insert_cardio_score_data_task',
    python_callable=insert_cardio_score_data,
    provide_context=True,
    dag=dag,
)

fetch_heart_rate_variability_data_task = PythonOperator(
    task_id='fetch_heart_rate_variability_data_task',
    python_callable=fetch_heart_rate_variability_data,
    provide_context=True,
    dag=dag,
)

insert_heart_rate_variability_data_task = PythonOperator(
    task_id='load_insert_heart_rate_variability_data_task',
    python_callable=insert_heart_rate_variability_data,
    provide_context=True,
    dag=dag,
)

fetch_heart_rate_data_task = PythonOperator(
    task_id='fetch_heart_rate_data_task',
    python_callable=fetch_heart_rate_data,
    provide_context=True,
    dag=dag,
)

transform_heart_rate_data_task = PythonOperator(
    task_id='transform_heart_rate_data_task',
    python_callable=transform_heart_rate_data,
    provide_context=True,
    dag=dag,
)

insert_heart_rate_data_task = PythonOperator(
    task_id='load_insert_heart_rate_data_task',
    python_callable=insert_heart_rate_data,
    provide_context=True,
    dag=dag,
)

# Fetch and merge fitbit data
fetch_activity_data_task = PythonOperator(
    task_id='fetch_activity_data_task',
    python_callable=fetch_activity_data,
    provide_context=True,
    dag=dag
)

transform_activities_data_task = PythonOperator(
    task_id='transform_activities_data_task',
    python_callable=transform_activities_data,
    provide_context=True,
    dag=dag,
)

# Insert merged data into the database
insert_activities_data_task = PythonOperator(
    task_id='insert_combined_activities_data',
    python_callable=insert_combined_activities_data
)

# Set task dependencies
refresh_tokens_task >> fetch_tokens_task >> [fetch_activity_data_task,
                                             fetch_skin_temperature_data_task,
                                             fetch_breathing_rate_data_task,
                                             fetch_sleep_data_task,
                                             fetch_cardio_score_data_task,
                                             fetch_heart_rate_variability_data_task,
                                             fetch_heart_rate_data_task]

fetch_activity_data_task >> transform_activities_data_task >> insert_activities_data_task

fetch_sleep_data_task >> [transform_sleep_data_task,
                          transform_sleep_levels_data_task]

fetch_skin_temperature_data_task >> insert_skin_temperature_data_task
fetch_breathing_rate_data_task >> insert_breathing_rate_data_task
fetch_cardio_score_data_task >> insert_cardio_score_data_task
fetch_heart_rate_variability_data_task >> insert_heart_rate_variability_data_task
fetch_heart_rate_data_task >> transform_heart_rate_data_task >> insert_heart_rate_data_task

transform_sleep_data_task >> insert_sleep_data_task
transform_sleep_levels_data_task >> insert_sleep_levels_data_task
