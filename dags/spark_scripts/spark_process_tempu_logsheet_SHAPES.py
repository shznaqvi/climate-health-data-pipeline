import argparse
import glob
import logging
import os
import pyodbc
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Target columns matching database schema exactly
TARGET_DB_COLUMNS = [
    "region",
    "cluster",
    "intervention_arm",
    "intervention_phase",
    "device_id",
    "devices_locations",
    "participant_id",
    "device_provided_datetime",
    "device_removed_datetime",
    "final_use",
    "data_extracted_datetime",
    "data_filename",
    "remarks",
    "filename",
]


def get_sql_connection(db_server, db_name, db_user, db_password):
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={db_server};"
        f"DATABASE={db_name};"
        f"UID={db_user};"
        f"PWD={db_password};"
        "Encrypt=no;TrustServerCertificate=yes;"
    )
    try:
        return pyodbc.connect(conn_str, timeout=15)
    except pyodbc.Error as e:
        logging.error(f"SQL Connection error: {e}")
        raise


def clean_and_deduplicate_columns(df):
    """Cleans string headers, applies column mapping, and merges duplicate columns per file."""
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[\n\r\t]", "_", regex=True)
        .str.replace(" ", "_")
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
        .str.replace("/", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )

    rename_map = {
        "s.no": "s_no",
        "s_no.": "s_no",
        "cluster_#": "cluster",
        "cluster_#.": "cluster",
        "cluster_id": "cluster",
        "hh_id": "participant_id",
        "device_no.": "device_number",
        "device_no": "device_number",
        "tempu_device_id": "device_id",
        "case__control": "intervention_arm",
        "intervention_arm_intervention_control": "intervention_arm",
        "intervension_phase_pre_post": "intervention_phase",
        "intervention_phase_pre_post": "intervention_phase",
        "device_installation_date": "device_provided_date",
        "device_installation_time": "device_provided_time",
        "device_return_date": "device_removed_date",
        "device_return_time": "device_removed_time",
        "device_provided_date_device_given": "device_provided_date",
        "device_removed_date_device_taken_back": "device_removed_date",
        "final_use_y_blank": "final_use",
        "location": "devices_locations",
        "device_location": "devices_locations",
    }
    df = df.rename(columns=rename_map)

    # Merge duplicate column names across identical mapped headers
    if df.columns.has_duplicates:
        deduped_cols = []
        for col_name in df.columns.unique():
            sub = df.loc[:, df.columns == col_name]
            if sub.shape[1] > 1:
                combined = sub.bfill(axis=1).iloc[:, 0].rename(col_name)
                deduped_cols.append(combined)
            else:
                deduped_cols.append(sub.iloc[:, 0])
        df = pd.concat(deduped_cols, axis=1)

    # Fallback: if device_id is missing, populate from device_number
    if "device_id" not in df.columns and "device_number" in df.columns:
        df["device_id"] = df["device_number"]
    elif "device_id" in df.columns and "device_number" in df.columns:
        df["device_id"] = df["device_id"].fillna(df["device_number"])
        df = df.drop(columns=["device_number"])

    return df


def combine_and_parse_datetime(df, date_col, time_col, target_col):
    """Safely combines date and time string columns into a standardized timestamp."""
    if date_col in df.columns and time_col in df.columns:
        date_str = df[date_col].fillna("").astype(str).str.strip()
        time_str = df[time_col].fillna("").astype(str).str.strip()
        time_str = time_str.replace(["", "nan", "None", "<NA>", "0", "00:00:00"], "00:00")
        dt_str = date_str + " " + time_str
        df[target_col] = pd.to_datetime(dt_str, format="mixed", errors="coerce")
    elif target_col in df.columns:
        df[target_col] = pd.to_datetime(df[target_col], format="mixed", errors="coerce")
    else:
        df[target_col] = pd.NaT
    return df


def remove_existing_db_records(conn, clean_df, target_table="tempu_data_logsheet_shapes"):
    """Queries SQL Server via active conn and removes records that already exist in the database."""
    if clean_df.empty or "device_id" not in clean_df.columns:
        return clean_df

    device_ids = clean_df["device_id"].dropna().unique().tolist()
    if not device_ids:
        return clean_df

    cursor = conn.cursor()
    chunk_size = 1000
    existing_pairs = set()

    for i in range(0, len(device_ids), chunk_size):
        chunk = device_ids[i: i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        query = f"""
            SELECT [device_id], CONVERT(VARCHAR(19), [device_provided_datetime], 120)
            FROM {target_table}
            WHERE [device_id] IN ({placeholders})
        """
        cursor.execute(query, chunk)
        for dev_id, dt_str in cursor.fetchall():
            existing_pairs.add((str(dev_id).strip(), str(dt_str).strip() if dt_str else None))

    cursor.close()

    if not existing_pairs:
        return clean_df

    def is_in_db(row):
        dev_id = str(row["device_id"]).strip() if pd.notnull(row["device_id"]) else ""
        dt_val = (
            str(row["device_provided_datetime"]).strip()
            if pd.notnull(row["device_provided_datetime"])
            else None
        )
        return (dev_id, dt_val) in existing_pairs

    initial_count = len(clean_df)
    clean_df_filtered = clean_df[~clean_df.apply(is_in_db, axis=1)].copy()
    skipped_count = initial_count - len(clean_df_filtered)

    if skipped_count > 0:
        logging.warning(
            f"Filtered out {skipped_count} record(s) that already exist in database table '{target_table}'."
        )

    return clean_df_filtered


def execute_bulk_insert_with_fallback(conn, insert_query, records):
    """Executes fast_executemany using existing conn; falls back to row-by-row on duplicate key errors."""
    cursor = conn.cursor()

    try:
        logging.info("Attempting fast bulk load via cursor.fast_executemany...")
        cursor.fast_executemany = True
        cursor.executemany(insert_query, records)
        conn.commit()
        logging.info(f"Successfully inserted {len(records)} records in bulk.")
    except pyodbc.IntegrityError as e:
        conn.rollback()
        logging.warning(
            f"Bulk insert encountered duplicate keys ({e}). Falling back to row-by-row load..."
        )

        cursor.fast_executemany = False
        inserted_count = 0
        skipped_duplicates = 0

        for row in records:
            try:
                cursor.execute(insert_query, row)
                conn.commit()
                inserted_count += 1
            except pyodbc.IntegrityError:
                conn.rollback()
                skipped_duplicates += 1
            except Exception as row_err:
                conn.rollback()
                logging.error(f"Error inserting row {row}: {row_err}")

        logging.info(
            f"Fallback load complete: {inserted_count} new records inserted, {skipped_duplicates} duplicate records skipped."
        )
    finally:
        cursor.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--db-server", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    args = parser.parse_args()

    csv_files = glob.glob(os.path.join(args.input_dir, "*.csv"))
    if not csv_files:
        logging.info("No CSV files found in input directory.")
        return

    total_files = len(csv_files)
    logging.info(f"Found {total_files} CSV file(s) to process.")

    # Open database connection once for entire pipeline execution
    conn = get_sql_connection(args.db_server, args.db_name, args.db_user, args.db_password)

    try:
        # PROCESS EACH FILE INDIVIDUALLY
        for idx, file_path in enumerate(csv_files, start=1):
            filename = os.path.basename(file_path)

            logging.info("=" * 80)
            logging.info(f"▶ STARTING FILE [{idx}/{total_files}]: {filename}")
            logging.info("=" * 80)

            try:
                # 1. READ & CLEAN SINGLE FILE
                df = pd.read_csv(file_path, dtype=str)
                if df.empty:
                    logging.warning(f"File {filename} is empty. Skipping.")
                    continue

                cleaned_df = clean_and_deduplicate_columns(df)

                # 2. CONVERT DATETIMES
                cleaned_df = combine_and_parse_datetime(
                    cleaned_df, "device_provided_date", "device_provided_time", "device_provided_datetime"
                )
                cleaned_df = combine_and_parse_datetime(
                    cleaned_df, "device_removed_date", "device_removed_time", "device_removed_datetime"
                )
                cleaned_df = combine_and_parse_datetime(
                    cleaned_df, "data_extracted_date", "data_extracted_time", "data_extracted_datetime"
                )

                # 3. OVERLAP VALIDATION & QUARANTINE LOGIC
                if "device_id" in cleaned_df.columns and "device_provided_datetime" in cleaned_df.columns:
                    cleaned_df = cleaned_df.sort_values(
                        by=["device_id", "device_provided_datetime"]
                    ).reset_index(drop=True)

                    cleaned_df["prev_max_removed"] = (
                        cleaned_df.groupby("device_id")["device_removed_datetime"]
                        .cummax()
                        .groupby(cleaned_df["device_id"])
                        .shift(1)
                    )

                    cleaned_df["is_overlapping"] = (
                            cleaned_df["device_provided_datetime"] < cleaned_df["prev_max_removed"]
                    )
                else:
                    cleaned_df["is_overlapping"] = False

                clean_df = cleaned_df[~cleaned_df["is_overlapping"]].copy()
                quarantine_df = cleaned_df[cleaned_df["is_overlapping"]].copy()

                # --- IN-FILE DEDUPLICATION (Drop duplicate CSV rows for UX_tempu_nodup) ---
                # initial_clean_count = len(clean_df)
                # clean_df = clean_df.drop_duplicates(
                #     subset=["device_id", "device_provided_datetime"], keep="first"
                # )
                # in_file_dups = initial_clean_count - len(clean_df)
                # if in_file_dups > 0:
                #     logging.warning(
                #         f"[{filename}] Filtered out {in_file_dups} duplicate row(s) inside CSV matching (device_id, device_provided_datetime)."
                #     )
                #
                # logging.info(
                #     f"[{filename}] Validation complete: {len(clean_df)} clean unique records, {len(quarantine_df)} quarantined records."
                # )
                dup_mask = clean_df.duplicated(
                    subset=["device_id", "device_provided_datetime"], keep="first"
                )
                dup_rows = clean_df[dup_mask]

                if not dup_rows.empty:
                    logging.warning(
                        f"[{filename}] Filtered out {len(dup_rows)} duplicate row(s) inside CSV matching (device_id, device_provided_datetime):"
                    )
                    for _, row in dup_rows.iterrows():
                        s_no_val = row.get("s_no", "N/A")
                        dev_id = row.get("device_id", "N/A")
                        dev_dt = row.get("device_provided_datetime", "N/A")
                        logging.warning(
                            f"  --> [Duplicate Row Filtered] S.No: {s_no_val} | device_id: {dev_id} | device_provided_datetime: {dev_dt}"
                        )

                    # Keep only first occurrence of each unique key pair
                    clean_df = clean_df[~dup_mask].copy()

                # 4. ALIGN SCHEMA & WRITE CLEAN RECORDS TO SQL SERVER
                if not clean_df.empty:
                    for col in TARGET_DB_COLUMNS:
                        if col not in clean_df.columns:
                            clean_df[col] = None

                    clean_df = clean_df[TARGET_DB_COLUMNS].copy()

                    for dt_col in [
                        "device_provided_datetime",
                        "device_removed_datetime",
                        "data_extracted_datetime",
                    ]:
                        clean_df[dt_col] = clean_df[dt_col].dt.strftime("%Y-%m-%d %H:%M:%S")

                    # --- DATABASE PRE-FILTERING (Checks existing records using conn) ---
                    clean_df = remove_existing_db_records(conn, clean_df)

                    if clean_df.empty:
                        logging.info(f"[{filename}] All records already exist in SQL Server. Skipping insert.")
                    else:
                        clean_df_formatted = clean_df.astype(object).where(pd.notnull(clean_df), None)

                        target_table = "tempu_data_logsheet_shapes"
                        columns_sql = ", ".join([f"[{col}]" for col in TARGET_DB_COLUMNS])
                        placeholders = ", ".join(["?"] * len(TARGET_DB_COLUMNS))
                        insert_query = f"INSERT INTO {target_table} ({columns_sql}) VALUES ({placeholders})"

                        records = [tuple(row) for row in clean_df_formatted.to_numpy()]
                        execute_bulk_insert_with_fallback(conn, insert_query, records)

                # 5. WRITE QUARANTINE RECORDS (BOTH JSON & CSV)
                if not quarantine_df.empty:
                    quarantine_dir = os.path.join(args.input_dir, "quarantine")
                    os.makedirs(quarantine_dir, exist_ok=True)
                    base_name = os.path.splitext(filename)[0]

                    quarantine_json = os.path.join(quarantine_dir, f"quarantine_{base_name}.json")
                    quarantine_csv = os.path.join(quarantine_dir, f"quarantine_{base_name}.csv")

                    logging.info(f"[{filename}] Exporting quarantined records to JSON & CSV...")
                    quarantine_df.astype(str).to_json(quarantine_json, orient="records", indent=2)
                    quarantine_df.to_csv(quarantine_csv, index=False)

                logging.info(f"✔ FINISHED FILE [{idx}/{total_files}]: {filename}\n")

            except Exception as e:
                logging.error(f"❌ ERROR processing file {filename}: {e}", exc_info=True)
                raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
