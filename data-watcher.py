import os
import logging
import multiprocessing
import random
import sys
import time
from pymongo import MongoClient, errors
from openpyxl import load_workbook
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from collections import defaultdict
from worker_module import worker

# Logging setup
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration Constants (These could be moved to a separate config file or environment variables)
CONFIG = {
    "MONGO_URI": "mongodb://root:example@localhost:27017/admin",
    "DATABASE_NAME": "recordsDB",
    "COLLECTION_NAME": "xlsxData",
    "MAX_RETRIES": 3,
    "CHUNK_SIZE": 1000,
    "NUM_PROCESSES": 2 * multiprocessing.cpu_count(),
    "SCHEMA_DETERMINATION_SAMPLE_SIZE": None,  # Will be calculated later
    "SCHEMA_THRESHOLD": None,  # Will be calculated later
    "WATCH_DIRECTORY": os.path.expanduser("~/downloaded_files/.xlsx/")
}


def connect_to_mongodb():
    return MongoClient(CONFIG["MONGO_URI"])


# Counts the number of Excel files in the directory.
def count_xlsx_files(directory):
    return len([f for f in os.listdir(directory) if f.endswith('.xlsx')])


def get_scaled_threshold(file_count):
    # The function `math.log1p(file_count)` calculates the natural logarithm of `1 + file_count`.
    # This ensures a positive and progressively increasing value. As the dataset size (file_count)
    # increases, the term `decay_factor / math.log1p(file_count)` reduces, making the threshold
    # move closer to the base_threshold. This ensures that the threshold is always greater than
    # the base_threshold.

    base_threshold = 0.5
    decay_factor = 0.1
    return base_threshold + decay_factor / (1 + file_count)


# Converts an Excel file to a list of dictionaries.
def xlsx_to_json(file_path, schema):
    with load_workbook(filename=file_path, read_only=True) as wb:
        ws = wb.active
        data = []

        for row in ws.iter_rows(values_only=True):
            filtered_row = {
                ws.cell(row=1, column=idx + 1).value: cell for idx, cell in enumerate(row) if
                ws.cell(row=1, column=idx + 1).value in schema and cell
            }
            if filtered_row:
                data.append(filtered_row)
    return data


# Removes empty keys and values from the records.
def sanitize_data(records):
    return [{k: v for k, v in record.items() if k and v} for record in records]


# Inserts records into MongoDB.
def insert_into_database(records, file_path):
    valid_records = [record for record in records if record and any(record.values())]
    if not valid_records:
        return
    retries = 0
    client = connect_to_mongodb()
    collection = client[CONFIG["DATABASE_NAME"]][CONFIG["COLLECTION_NAME"]]
    while retries < CONFIG["MAX_RETRIES"]:
        try:
            collection.insert_many(valid_records)
            break
        except errors.ServerSelectionTimeoutError:
            logger.error(f"Server selection timeout error when inserting into MongoDB for {file_path}. Retrying...")
            retries += 1
            time.sleep(3 * retries)  # Exponential back-off
        except (errors.OperationFailure, Exception) as e:
            logger.error(f"Error when inserting into MongoDB for {file_path}. Error: {str(e)}")
            break
    client.close()


# Processes an Excel file.
def process_xlsx(file_path):
    try:
        data = xlsx_to_json(file_path, CONFIG["SCHEMA"])
        data = sanitize_data(data)
        for i in range(0, len(data), CONFIG["CHUNK_SIZE"]):
            insert_into_database(data[i:i + CONFIG["CHUNK_SIZE"]], file_path)
        logger.info(f"Successfully processed {file_path}")
    except Exception as e:
        logger.error(f"Error processing {file_path}. Error: {str(e)}")


# Determines the common schema for all Excel files in the directory.
def determine_schema(directory):
    header_counts = defaultdict(int)
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.xlsx')]
    sampled_files = random.sample(files, min(CONFIG["SCHEMA_DETERMINATION_SAMPLE_SIZE"], len(files)))

    for file_path in sampled_files:
        logger.info(f"Processing: {file_path}")
        wb = load_workbook(filename=file_path, read_only=True)
        ws = wb.active
        headers = [cell for cell in next(ws.iter_rows(values_only=True)) if cell]
        for header in headers:
            header_counts[header] += 1

        wb.close()

    total_files = len(files)
    common_headers = [header for header, count in header_counts.items() if
                      count / total_files >= CONFIG["SCHEMA_THRESHOLD"]]

    return common_headers


# Watchdog event handler to process files as they are added to the directory.
class XLSXHandler(FileSystemEventHandler):
    def __init__(self, process_delay=60):
        super().__init__()
        self.process_delay = process_delay
        self.processing_files = set()
        self.file_lock = multiprocessing.Lock()

    def on_modified(self, event):
        if event.is_directory or not event.src_path.endswith('.xlsx'):
            return

        with self.file_lock:
            if event.src_path not in self.processing_files:
                self.processing_files.add(event.src_path)
                file_queue.put(event.src_path)
                time.sleep(self.process_delay)
                self.processing_files.remove(event.src_path)
            else:
                file_queue.put(event.src_path)


if __name__ == "__main__":
    if not os.path.exists(CONFIG["WATCH_DIRECTORY"]):
        logger.error(f"The directory {CONFIG['WATCH_DIRECTORY']} does not exist. Exiting program.")
        sys.exit(1)

    file_count = count_xlsx_files(CONFIG["WATCH_DIRECTORY"])
    CONFIG["SCHEMA_DETERMINATION_SAMPLE_SIZE"] = file_count
    CONFIG["SCHEMA_THRESHOLD"] = get_scaled_threshold(file_count)

    CONFIG["SCHEMA"] = determine_schema(CONFIG["WATCH_DIRECTORY"])
    if not CONFIG["SCHEMA"]:
        logger.error("No common schema determined. Exiting program.")
        sys.exit(1)

    file_queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(processes=CONFIG["NUM_PROCESSES"])
    for _ in range(CONFIG["NUM_PROCESSES"]):
        pool.apply_async(worker, args=(file_queue, process_xlsx,))

    existing_files = [os.path.join(CONFIG["WATCH_DIRECTORY"], f) for f in os.listdir(CONFIG["WATCH_DIRECTORY"]) if
                      f.endswith('.xlsx')]
    for f in existing_files:
        file_queue.put(f)

    event_handler = XLSXHandler(process_delay=60)
    observer = Observer()
    observer.schedule(event_handler, CONFIG["WATCH_DIRECTORY"], recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        for _ in range(CONFIG["NUM_PROCESSES"]):
            file_queue.put("STOP")
        observer.stop()
        pool.close()
        pool.join()

    observer.join()
