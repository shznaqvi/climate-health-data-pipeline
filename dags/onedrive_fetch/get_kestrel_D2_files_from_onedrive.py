import shutil
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

SOURCE_FOLDERS = [
    # Mithi
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Suneel Piryani's files - Validation_Device_Mithi\Office_Device_Mithi\Kestrel5400",

    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Suneel Piryani's files - Validation_Device_Mithi\Office_Device_Mithi\KestrelDropD2",
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Suneel Piryani's files - Validation_Device_Mithi\Cluster_Device_Mithi\KestrelDrop2",
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Suneel Piryani's files - Mithi Drop2 devices data",

    # TMK
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Suneel Piryani's files - Validation_Devices_TM-Khan\Office_Devices_TM-Khan\Kestrel5400",

    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Suneel Piryani's files - Validation_Devices_TM-Khan\Office_Devices_TM-Khan\KestrelDrop2",
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Suneel Piryani's files - Validation_Devices_TM-Khan\Cluster_Ambient_Devices_TM-Khan\KestrelDrop2",
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Suneel Piryani's files - Validation_Devices_TM-Khan\Participant_Validation_Devices\KestrelDrop2",

    # Karachi
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Muhammad Asghar's files - TEMP-U (H.H)\Validation Devices Karachi\Office_Devices_Karachi\Kestrel5400",

    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Muhammad Asghar's files - TEMP-U (H.H)\Validation Devices Karachi\Office_Devices_Karachi\KestrelDropD2",
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Muhammad Asghar's files - TEMP-U (H.H)\Validation Devices Karachi\Cluster_Ambient_Devices_Karachi\KestrelDropD2",
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Muhammad Asghar's files - TEMP-U (H.H)\Karachi TempU03 Data\KestrelDropD2",

    # Matiari

    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Abdullah Memon's files - Device Data_ Tempu & Kestil\Devices Installed Office\Kestrel5400",

    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Abdullah Memon's files - Device Data_ Tempu & Kestil\Devices Installed Office\KestredDropD2",
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Abdullah Memon's files - Device Data_ Tempu & Kestil\Cluster_Ambient_Devices_Matiari\Kestrel_D2",
    r"C:\Users\hassan.naqvi\OneDrive - Aga Khan University\Abdullah Memon's files - Device Data_ Tempu & Kestil\Village Devices\KestrelDropD2",

    # GB
    r"G:\My Drive\GB-Devices-Data\AKHSP\KestrelDropD2",
    r"G:\My Drive\GB-Devices-Data\EPA\KestrelDropD2"

]
# === CONFIGURATION ===
DESTINATION_FOLDER = "../dags/csv/kestrel_data"
ALLOWED_EXTENSION = ".csv"

MAX_RETRIES = 3
RETRY_DELAY = 3

# Thread control (important for OneDrive stability)
MAX_WORKERS = max(2, (os.cpu_count() or 4) - 1)

logging.basicConfig(
    filename='../copy_csv.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

os.makedirs(DESTINATION_FOLDER, exist_ok=True)


def sanitize(name):
    return name.replace(" ", "_").replace("'", "").replace('"', '').strip()


def get_prefixed_filename(subfolder, filename):
    if not subfolder or subfolder == ".":
        return sanitize(filename)
    return f"{sanitize(subfolder)}_{sanitize(filename)}"


def process_single_file(task):
    source_file, destination_file, done_subfolder, file = task

    print(f"--- Processing: {file}")

    try:
        # Skip duplicates
        if os.path.exists(destination_file):
            print(f"----- Skipped duplicate: {destination_file}")
            return False

        # Copy with retry
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                shutil.copy2(source_file, destination_file)
                print(f"----- Copied: {file}")
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    print(f"⚠️ Copy attempt {attempt} failed: {e}")
                    time.sleep(RETRY_DELAY)
                else:
                    raise e

        # Move with retry
        os.makedirs(done_subfolder, exist_ok=True)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                shutil.move(source_file, os.path.join(done_subfolder, file))
                print(f"----- Moved to Done: {file}")
                return True
            except Exception as e:
                if attempt < MAX_RETRIES:
                    print(f"⚠️ Move attempt {attempt} failed: {e}")
                    time.sleep(RETRY_DELAY)
                else:
                    raise e

    except Exception as e:
        print(f"❌ Error processing {file}: {e}")
        logging.error(f"{source_file}: {e}")
        return False


def process_csv_files_parallel():
    print("🚀 Starting multi-threaded processing...")

    tasks = []

    # 🔍 Step 1: Collect all files first
    for source_folder in SOURCE_FOLDERS:
        print(f"- Scanning folder: {source_folder}")

        new_data_folder = os.path.join(source_folder, "New_Data")
        done_folder = os.path.join(source_folder, "Done")

        if not os.path.exists(new_data_folder):
            print(f"⚠️ Missing New_Data: {new_data_folder}")
            continue

        for root, _, files in os.walk(new_data_folder):
            rel_path = os.path.relpath(root, new_data_folder)

            if rel_path.lower().startswith("done"):
                continue

            subfolder_name = os.path.basename(rel_path)

            for file in files:
                if not file.lower().endswith(ALLOWED_EXTENSION):
                    continue

                source_file = os.path.join(root, file)
                new_filename = get_prefixed_filename(subfolder_name, file)
                destination_file = os.path.join(DESTINATION_FOLDER, new_filename)

                done_subfolder = os.path.join(done_folder, rel_path)

                tasks.append((source_file, destination_file, done_subfolder, file))

    print(f"📦 Total files to process: {len(tasks)}")

    # ⚡ Step 2: Run in parallel
    success = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_single_file, task) for task in tasks]

        for future in as_completed(futures):
            if future.result():
                success += 1

    print(f"\n✅ Completed | Success: {success} | Failed: {len(tasks) - success}")


# === RUN ===
if __name__ == "__main__":
    process_csv_files_parallel()
