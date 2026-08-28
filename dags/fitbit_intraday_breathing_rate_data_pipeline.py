import base64
import os
import requests
import pyodbc
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

logging = LoggingMixin().log  # Use Airflow's logger

# Fetch the database connection details from environment variables
DB_SERVER = os.getenv('DB_SERVER', 'host.docker.internal')  # Default if not found
DB_USER = os.getenv('DB_USER', 'sa')  # Default if not found
DB_PASSWORD = os.getenv('DB_PASSWORD', 'YourStrong!Passw0rd')  # Default if not found
DB_NAME = os.getenv('DB_NAME', 'ighd')  # Default if not found


# Define the connection to SQL Server
def get_sql_connection():
    """
    Establish a connection to the SQL Server database using pyodbc.
    """
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
        conn = pyodbc.connect(conn_str, timeout=10)
        logging.info("    > SQL Connection established successfully.")
        return conn
    except pyodbc.InterfaceError as e:
        logging.error(f"Interface error while connecting to SQL Server: {e}")
    except pyodbc.DatabaseError as e:
        logging.error(f"Database error while connecting to SQL Server: {e}")
    except pyodbc.Error as e:
        logging.error(f"General pyodbc error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return None  # Return None if connection fails


# Function to refresh access token using refresh token
def refresh_access_token(refresh_token, context):
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
        logging.error(f"Error refreshing token: {response.text}")
        return None, None


# Function to refresh tokens for all users in the database
def refresh_tokens(**kwargs):
    context = kwargs.get('context', None)
    logging.info("Refreshing tokens for all users...")
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        # Fetch all tokens from the fitbit_tokens table
        cursor.execute("SELECT fitbit_user_id, refresh_token FROM fitbit_tokens")
        tokens = cursor.fetchall()

        if not tokens:
            logging.warning("No tokens found in the database.")
            return

        for token_data in tokens:
            fitbit_user_id = token_data[0]
            refresh_token = token_data[1]

            logging.info(f"Processing Fitbit User ID: {fitbit_user_id}")

            # Refresh token
            new_access_token, new_refresh_token = refresh_access_token(refresh_token, context)

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
                logging.error(f"Failed to refresh tokens for Fitbit User ID: {fitbit_user_id}")

    except pyodbc.Error as e:
        logging.error(f"Error with SQL connection: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
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
                cursor.execute("SELECT fitbit_user_id, access_token FROM fitbit_tokens")
                tokens = cursor.fetchall()

        if not tokens:
            logging.warning("No tokens returned from the database.")
            raise ValueError("No tokens found in the database.")  # Fail if no tokens

        token_dict = [
            {"fitbit_user_id": row[0], "access_token": row[1]} for row in tokens
        ]
        logging.info(f"Fetched {len(tokens)} tokens.")

        # Push tokens to XCom
        kwargs["ti"].xcom_push(key="tokens", value=token_dict)
        logging.info("Pushed tokens to XCom.")

        return token_dict

    except pyodbc.Error as db_error:
        logging.error(f"Database error: {db_error}")
        raise  # Reraise the database error to fail the task
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise  # Reraise the exception to fail the task


# Function to fetch intraday breathing rate data from Fitbit API
def fetch_intraday_breathing_rate_data(**kwargs):
    try:
        ti = kwargs['ti']  # Get task instance
        tokens = ti.xcom_pull(key='tokens', task_ids='fetch_tokens')
        if not tokens:
            raise ValueError("No tokens available for API check.")

        all_data = []

        days30 = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        days30 = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=61)).strftime('%Y-%m-%d')

        for user_data in tokens:
            fitbit_user_id = user_data.get('fitbit_user_id')
            access_token = user_data.get('access_token')

            if not fitbit_user_id or not access_token:
                logging.error(f"Missing fitbit_user_id or access_token for user {user_data}")
                continue
            else:
                logging.info(f"Fetching intraday breathing rate data for {fitbit_user_id}")

            api_url = f"https://api.fitbit.com/1/user/-/br/date/{days30}/{yesterday}/all.json"
            logging.info(f'API URL: {api_url}')

            headers = {"Authorization": f"Bearer {access_token}"}

            response = requests.get(api_url, headers=headers)
            logging.info(f"API Response for user {fitbit_user_id}: {response.status_code}")

            data = response.json()
            if response.status_code == 200:
                if 'br' in data and isinstance(data['br'], list):
                    all_data.append({
                        "fitbit_user_id": fitbit_user_id,
                        "br": data['br']  # Intraday breathing rate data
                    })
                else:
                    logging.error(f"No intraday breathing rate data for user {fitbit_user_id}.")
            else:
                error_type = data.get('errors', [{}])[0].get('errorType', 'Unknown')
                logging.error(
                    f"Error fetching data for user {fitbit_user_id}: {response.status_code}, "
                    f"Error Type: {error_type}"
                )

        if not all_data:
            raise ValueError("No intraday breathing rate data collected for any user.")

        # Push the gathered data to XCom
        ti.xcom_push(key='intraday_breathing_rate_data', value=all_data)
        logging.info(f"Intraday breathing rate data pushed to XCom: {all_data}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        raise


# Transform JSON response
def transform_intraday_breathing_rate_data(**kwargs):
    ti = kwargs['ti']
    breathing_rate_data = ti.xcom_pull(key='intraday_breathing_rate_data',
                                       task_ids='fetch_intraday_breathing_rate_data')
    if not breathing_rate_data:
        raise ValueError("No intraday breathing rate data available to transform.")

    transformed_data = []

    for user in breathing_rate_data:
        fitbit_user_id = user["fitbit_user_id"]
        if not user or "br" not in user:
            raise ValueError("No valid breathing rate data found.")
        for record in user["br"]:
            transformed_data.append({
                "fitbit_user_id": fitbit_user_id,
                "date_time": record["dateTime"],
                "deep_sleep_br": record["value"]["deepSleepSummary"]["breathingRate"],
                "rem_sleep_br": record["value"]["remSleepSummary"]["breathingRate"],
                "full_sleep_br": record["value"]["fullSleepSummary"]["breathingRate"],
                "light_sleep_br": record["value"]["lightSleepSummary"]["breathingRate"]
            })

    # Push transformed data to XCom for next task
    ti.xcom_push(key='transformed_intraday_breathing_rate_data', value=transformed_data)


# Function to insert intraday breathing rate data into the database
def insert_intraday_breathing_rate_data(**kwargs):
    logging.info("Starting intraday breathing rate data insertion...")

    try:
        ti = kwargs['ti']
        breathing_rate_data = ti.xcom_pull(key='transformed_intraday_breathing_rate_data',
                                           task_ids='transform_intraday_breathing_rate_data')

        if not breathing_rate_data:
            raise ValueError("No intraday breathing rate data available to insert.")

        conn = get_sql_connection()
        cursor = conn.cursor()

        for record in breathing_rate_data:
            fitbit_user_id = record['fitbit_user_id']
            date_time = record['date_time']
            deep_sleep_br = record['deep_sleep_br']
            rem_sleep_br = record['rem_sleep_br']
            full_sleep_br = record['full_sleep_br']
            light_sleep_br = record['light_sleep_br']

            if all(value is None for value in [deep_sleep_br, rem_sleep_br, full_sleep_br, light_sleep_br]):
                logging.warning(f"Skipping null 'breathing rate' for {fitbit_user_id} on {date_time}.")
                continue

            # Check for duplicate entry (unique constraint check)
            cursor.execute(
                """SELECT COUNT(*)
                   FROM fitbit_intraday_breathing_rate
                   WHERE date_time = ?
                     AND fitbit_user_id = ?""",
                (date_time, fitbit_user_id)
            )
            if cursor.fetchone()[0] > 0:
                logging.warning(f"Duplicate data detected for {fitbit_user_id} on {date_time}. Skipping.")
                continue

            # Insert new data
            logging.info(f"Inserting intraday breathing rate data for {fitbit_user_id} on {date_time}.")

            cursor.execute(
                """INSERT INTO fitbit_intraday_breathing_rate
                   (date_time, fitbit_user_id, deep_sleep_br, rem_sleep_br, full_sleep_br, light_sleep_br)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (date_time, fitbit_user_id, deep_sleep_br, rem_sleep_br, full_sleep_br, light_sleep_br)
            )

        conn.commit()
        logging.info("Intraday breathing rate data insertion completed successfully.")
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


# Define the DAG
dag = DAG(
    'fitbit_intraday_breathing_rate_data_pipeline',
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG to fetch and process Fitbit data',
    schedule='@daily',  # Updated to 'schedule' instead of 'schedule_interval'
    start_date=datetime(2024, 11, 1),
    catchup=False
)

# Airflow task to refresh tokens
refresh_tokens_task = PythonOperator(
    task_id='refresh_tokens',
    python_callable=refresh_tokens,
    dag=dag
)

# Airflow task to fetch tokens
fetch_tokens_task = PythonOperator(
    task_id='fetch_tokens',
    python_callable=fetch_tokens,
    provide_context=True,
    dag=dag
)

# Airflow task to fetch intraday breathing rate data
fetch_intraday_breathing_rate_task = PythonOperator(
    task_id='fetch_intraday_breathing_rate_data',
    python_callable=fetch_intraday_breathing_rate_data,
    provide_context=True,
    dag=dag
)

# Airflow task to insert intraday breathing rate data
transform_intraday_breathing_rate_task = PythonOperator(
    task_id='transform_intraday_breathing_rate_data',
    python_callable=transform_intraday_breathing_rate_data,
    provide_context=True,
    dag=dag
)

# Airflow task to insert intraday breathing rate data
insert_intraday_breathing_rate_task = PythonOperator(
    task_id='insert_intraday_breathing_rate_data',
    python_callable=insert_intraday_breathing_rate_data,
    provide_context=True,
    dag=dag
)

# Set task dependencies
refresh_tokens_task >> fetch_tokens_task >> fetch_intraday_breathing_rate_task >> transform_intraday_breathing_rate_task >> insert_intraday_breathing_rate_task
