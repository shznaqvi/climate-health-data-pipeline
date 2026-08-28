from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import os
import re
import shutil
import logging
import pandas as pd
import pyodbc

# =========================
# PATHS
# =========================
BASE_PATH = "/opt/airflow/dags/csv/weather_station_data"
DONE_DIR = os.path.join(BASE_PATH, "done")
TEMP_DIR = os.path.join(BASE_PATH, "temp")

SAME_OLD_DIR = os.path.join(BASE_PATH, "same_old")


# =========================
# EXTRACT
# =========================
def extract(**kwargs):
    files = glob.glob(os.path.join(FOLDER_PATH, '*.xlsx'))
    if not files:
        raise DataExtractionError("No Excel files found for extraction.")

    logging.info(f"Found {len(files)} Excel files for extraction.")
    kwargs['ti'].xcom_push(key='file_list', value=files)


# =========================
# HEADER PARSER
# =========================
def parse_header(col):
    if col.startswith("#") or "Date Time" in col:
        return {"field": col, "unit": None, "lgr": None}

    col = col.replace("Â°C", "°C").replace("Ã¸", "ø")

    field = col.split(",")[0].strip()

    unit = None

    # CASE 1: unit after comma
    m = re.search(r",\s*([^()]+?)\s*\(", col)
    if m:
        unit = m.group(1).strip()
    else:
        m2 = re.search(r",\s*([^()]+)$", col)
        if m2:
            unit = m2.group(1).strip()

    # CASE 2: fallback parentheses
    if not unit:
        m3 = re.search(r"\(([^)]+)\)", col)
        if m3:
            unit = m3.group(1)

    lgr = re.search(r"LGR S/N:\s*(\d+)", col)

    return {
        "field": field,
        "unit": unit,
        "lgr": lgr.group(1) if lgr else None
    }


# =========================
# TRANSFORM
# =========================
def transform(**context):
    data = context["ti"].xcom_pull(task_ids="extract_task")
    file_path = data["file_path"]
    df = pd.read_json(data["df"])

    logging.info(f"Transforming file: {file_path}")

    # metadata extraction
    lgr_id = None
    for c in df.columns:
        meta = parse_header(c)
        if meta["lgr"]:
            lgr_id = meta["lgr"]
            break

    df["FileName"] = os.path.basename(file_path)
    df["LGR_DeviceID"] = lgr_id
    df["TimeZone"] = "GMT+05:00"

    # datetime parsing
    dt_col = [c for c in df.columns if "Date Time" in c]
    if dt_col:
        df["LogDateTime"] = pd.to_datetime(df[dt_col[0]], errors="coerce")

    return {
        "file_path": file_path,
        "df": df.to_json(orient="records")
    }


# =========================
# LOAD
# =========================
def get_conn():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=sqlserver;"
        "DATABASE=weather_db;"
        "UID=sa;"
        "PWD=YourPassword;"
        "TrustServerCertificate=yes"
    )


def load(**context):
    data = context["ti"].xcom_pull(task_ids="transform_task")
    file_path = data["file_path"]
    df = pd.read_json(data["df"])

    logging.info(f"Loading file: {file_path}")

    conn = get_conn()
    cursor = conn.cursor()

    insert_sql = """
                 INSERT INTO hobo_weather_data (LogDateTime,
                                                TimeZone,
                                                LGR_DeviceID,
                                                PlotTitle,
                                                FileName)
                 VALUES (?, ?, ?, ?, ?) \
                 """

    rows = []
    inserted = 0

    for _, r in df.iterrows():

        row = (
            r.get("LogDateTime"),
            r.get("TimeZone"),
            r.get("LGR_DeviceID"),
            r.get("PlotTitle"),
            r.get("FileName")
        )

        logging.info(f"ROW: {row}")

        if any(row):
            rows.append(row)

    try:
        cursor.executemany(insert_sql, rows)
        conn.commit()
        inserted = len(rows)
    except Exception as e:
        logging.error(str(e))
        raise

    return {"file_path": file_path, "inserted": inserted}


# =========================
# POST PROCESS
# =========================
def post_process(**context):
    result = context["ti"].xcom_pull(task_ids="load_task")

    file_path = result["file_path"]
    inserted = result["inserted"]

    file_name = os.path.basename(file_path)

    if inserted == 0:
        target = os.path.join(SAME_OLD_DIR, file_name)
        logging.warning(f"No new rows → SAME_OLD: {file_name}")
    else:
        target = os.path.join(DONE_DIR, file_name)

    shutil.move(file_path, target)
    logging.info(f"Moved to: {target}")


# =========================
# DAG
# =========================
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
        dag_id="hobo_etl_pipeline",
        default_args=default_args,
        schedule_interval=None,
        catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    post_task = PythonOperator(
        task_id="post_task",
        python_callable=post_process
    )

    extract_task >> transform_task >> load_task >> post_task
