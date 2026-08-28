import base64
import os
import requests
import pyodbc
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import time

logging = LoggingMixin().log  # Use Airflow's logger

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
        return None


def refresh_access_token(refresh_token):
    client_id = "23PL52"
    client_secret = "6407ab0c90917ee8a7e4cfa7da694027"
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
        logging.error(f"Error refreshing token: {response.text}")
        return None, None


def refresh_tokens(**kwargs):
    logging.info("Refreshing tokens for all users...")
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT fitbit_user_id, refresh_token FROM fitbit_tokens")
        tokens = cursor.fetchall()
        if not tokens:
            logging.warning("No tokens found in the database.")
            return
        for token_data in tokens:
            fitbit_user_id, refresh_token = token_data
            logging.info(f"Processing Fitbit User ID: {fitbit_user_id}")
            new_access_token, new_refresh_token = refresh_access_token(refresh_token)
            if new_access_token and new_refresh_token:
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
                logging.error(f"Failed to refresh tokens for Fitbit User ID: {fitbit_user_id}")
    finally:
        if conn:
            conn.close()


def fetch_tokens(**kwargs):
    logging.info("Fetching Tokens...")
    try:
        with get_sql_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT fitbit_user_id, access_token FROM fitbit_tokens where fitbit_user_id not like ?",
                               ("@aku"))
                tokens = cursor.fetchall()
        if not tokens:
            raise ValueError("No tokens found in the database.")
        token_dict = [{"fitbit_user_id": row[0], "access_token": row[1]} for row in tokens]
        kwargs["ti"].xcom_push(key="tokens", value=token_dict)
        return token_dict
    except pyodbc.Error as db_error:
        logging.error(f"Database error: {db_error}")
        raise


def fetch_intraday_heart_rate_data(**kwargs):
    try:
        ti = kwargs['ti']
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError("No tokens available for API check.")

        heart_rate_data = []
        # days30 = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        # yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        # days30 = (datetime.now() - timedelta(days=110)).strftime('%Y-%m-%d')
        # days3 = (datetime.now() - timedelta(days=110)).strftime('%Y-%m-%d')
        # yesterday = (datetime.now() - timedelta(days=86)).strftime('%Y-%m-%d')

        days30 = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        # For a particular period of 30-days ending on this date
        # this_date = datetime(2025, 8, 27)
        # days30 = (this_date - timedelta(days=30)).strftime('%Y-%m-%d')
        # yesterday = this_date.strftime('%Y-%m-%d')

        days30_date = datetime.strptime(days30, '%Y-%m-%d')
        yesterday_date = datetime.strptime(yesterday, '%Y-%m-%d')

        for single_date in (days30_date + timedelta(n) for n in range((yesterday_date - days30_date).days + 1)):
            date_str = single_date.strftime('%Y-%m-%d')
            logging.info(f"Processing for date: {date_str}")
            for user_data in tokens:
                fitbit_user_id = user_data.get('fitbit_user_id')
                access_token = user_data.get('access_token')

                if not fitbit_user_id or not access_token:
                    logging.error(f"Missing fitbit_user_id or access_token for user {user_data}")
                    continue
                # for single_date in (days30_date + timedelta(n) for n in range((yesterday_date - days30_date).days + 1)):
                #     date_str = single_date.strftime('%Y-%m-%d')
                api_url = f"https://api.fitbit.com/1/user/{fitbit_user_id}/activities/heart/date/{date_str}/{date_str}/1min.json"
                logging.info(f"Fetching {api_url}")
                headers = {"Authorization": f"Bearer {access_token}"}
                logging.info(f"Fetching data for user {fitbit_user_id} on {date_str}...")
                response = requests.get(api_url, headers=headers)

                # Log the response headers
                logging.info(f"Response headers for user {fitbit_user_id}: {response.headers}")

                data = response.json()
                if response.status_code == 200:
                    logging.info(f"Response received for user {fitbit_user_id}: {data}")
                    date_time = data['activities-heart'][0]['dateTime']
                    if 'activities-heart-intraday' in data and data['activities-heart-intraday']['dataset']:
                        heart_rate_data.append({
                            "fitbit_user_id": fitbit_user_id,
                            "date_time": date_time,
                            "heart_rate": data['activities-heart-intraday']['dataset']
                        })
                    else:
                        logging.error(f"No intraday heart rate data for user {fitbit_user_id} on {date_str}.")

                elif response.status_code == 429:  # Rate limit exceeded
                    reset_time = int(response.headers.get('fitbit-rate-limit-reset', 60))  # Default 60s if missing
                    logging.warning(f"Rate limit exceeded! Waiting {reset_time} seconds before retrying...")
                    time.sleep(reset_time + 1)  # Wait before retrying

                else:
                    if not data.get('success', True):
                        error_message = data['errors'][0]['message']
                        logging.error(
                            f"Error fetching data for user {fitbit_user_id} on {date_str}: {response.status_code}, "
                            f"Error Type: {data['errors'][0]['errorType']}, Message: {error_message}"
                        )
                    else:
                        logging.error(
                            f"Error fetching data for user {fitbit_user_id} on {date_str}: {response.status_code}, "
                            f"Message: {response.text}"
                        )

        if not heart_rate_data:
            raise ValueError("No heart rate data collected.")

        ti.xcom_push(key='intraday_heart_rate_data', value=heart_rate_data)
        logging.info("Heart rate data pushed to XCom.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception: {e}")
        raise


from datetime import datetime


def transform_intraday_heart_rate_data(**kwargs):
    ti = kwargs['ti']
    heart_rate_data = ti.xcom_pull(
        key='intraday_heart_rate_data',
        task_ids='fetch_intraday_heart_rate_data'
    )
    if not heart_rate_data:
        raise ValueError("No intraday heart rate data available to transform.")

    transformed_data = []

    for user in heart_rate_data:
        fitbit_user_id = user.get("fitbit_user_id")
        date_str = user.get("date_time")  # e.g. "2025-08-28"

        if not user or "heart_rate" not in user:
            raise ValueError("No valid heart rate data found.")

        for record in user["heart_rate"]:
            time_str = record["time"]  # e.g. "08:03:00"
            hr_date_time = f"{date_str} {time_str}"

            # Optional: validate datetime format
            try:
                hr_date_time = datetime.strptime(hr_date_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise ValueError(f"Invalid datetime format: {hr_date_time}")

            transformed_data.append({
                "fitbit_user_id": fitbit_user_id,
                "heart_rate": record["value"],
                "hr_date_time": hr_date_time.strftime("%Y-%m-%d %H:%M:%S")
            })

    ti.xcom_push(key='transformed_intraday_heart_rate_data', value=transformed_data)


def insert_intraday_heart_rate_data(**kwargs):
    logging.info("Starting intraday heart rate data insertion...")

    try:
        ti = kwargs['ti']
        heart_rate_data = ti.xcom_pull(key='transformed_intraday_heart_rate_data',
                                       task_ids='transform_intraday_heart_rate_data')

        if not heart_rate_data:
            raise ValueError("No intraday heart rate data available to insert.")

        conn = get_sql_connection()
        cursor = conn.cursor()

        for record in heart_rate_data:
            fitbit_user_id = record['fitbit_user_id']
            hr_data_time = record['hr_date_time']
            heart_rate = record['heart_rate']

            if heart_rate is None:
                logging.warning(f"Skipping null 'heart rate' for {fitbit_user_id} on {date} at {time}.")
                continue

            cursor.execute(
                """SELECT COUNT(*)
                   FROM fitbit_intraday_heart_rate
                   WHERE hr_date_time = ?
                     AND fitbit_user_id = ?""",
                (hr_date_time, fitbit_user_id)
            )
            if cursor.fetchone()[0] > 0:
                logging.warning(f"Duplicate data detected for {fitbit_user_id} on {date} at {hr_time}. Skipping.")
                continue

            logging.info(f"Inserting intraday heart rate data for {fitbit_user_id} on {date} at {hr_time}.")

            cursor.execute(
                """INSERT INTO fitbit_intraday_heart_rate
                       (hr_date_time, fitbit_user_id, heart_rate)
                   VALUES (?, ?, ?)""",
                (hr_date_time, fitbit_user_id, heart_rate)
            )

        conn.commit()
        logging.info("Intraday heart rate data insertion completed successfully.")
    except pyodbc.DatabaseError as db_err:
        logging.error(f"Database error occurred: {db_err}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


dag = DAG(
    'fitbit_intraday_heart_rate_data_pipeline',
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG to fetch and process Fitbit intraday heart rate data',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

refresh_tokens_task = PythonOperator(
    task_id='refresh_tokens',
    python_callable=refresh_tokens,
    dag=dag
)

fetch_tokens_task = PythonOperator(
    task_id='fetch_tokens',
    python_callable=fetch_tokens,
    provide_context=True,
    dag=dag
)

fetch_intraday_heart_rate_task = PythonOperator(
    task_id='fetch_intraday_heart_rate_data',
    python_callable=fetch_intraday_heart_rate_data,
    provide_context=True,
    dag=dag
)

transform_intraday_heart_rate_task = PythonOperator(
    task_id='transform_intraday_heart_rate_data',
    python_callable=transform_intraday_heart_rate_data,
    provide_context=True,
    dag=dag
)

insert_intraday_heart_rate_task = PythonOperator(
    task_id='insert_intraday_heart_rate_data',
    python_callable=insert_intraday_heart_rate_data,
    provide_context=True,
    dag=dag
)

refresh_tokens_task >> fetch_tokens_task >> fetch_intraday_heart_rate_task >> transform_intraday_heart_rate_task >> insert_intraday_heart_rate_task
