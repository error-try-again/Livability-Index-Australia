import os
import sys
import time
import logging
import multiprocessing
import hashlib
import pandas as pd
from typing import Dict, Any
from openpyxl import Workbook
from pymongo import MongoClient, errors as mongo_errors
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Config:
    """
    A class that handles the loading of configuration values from the environment.

    Attributes:
        data (Dict[str, Any]): A dictionary containing the loaded configuration values.
    """

    def __init__(self):
        self.data: Dict[str, Any] = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration values from the environment, or use default values if not found.

        Returns:
            Dict[str, Any]: A dictionary of loaded configuration values.
        """
        return {key: os.environ.get(key, default) for key, default in self.default_values().items()}

    def get(self, key: str) -> Any:
        """
        Get a configuration value by its key.

        Args:
            key (str): The key for the desired configuration value.

        Returns:
            Any: The configuration value for the given key.
        """
        return self.data.get(key)

    @staticmethod
    def default_values() -> Dict[str, Any]:
        """
        Provide default configuration values.

        Returns:
            Dict[str, Any]: A dictionary of default configuration values.
        """
        return {
            'mongo_uri': "mongodb://root:example@localhost:27017/admin",
            'database_name': "recordsDB",
            'collection_name': "xlsxData",
            'max_retries': 3,
            'num_processes': multiprocessing.cpu_count(),
            'watch_directory': os.path.expanduser("~/downloaded_files/.xlsx/"),
            'buffer_size': 4096,
            'excel_header_rows': 5,
        }


class MongoDBHandler:
    """
    A class to handle operations related to MongoDB.

    Attributes:
        config (Config): An instance of the Config class to get configuration values.
        _client (MongoClient): A MongoDB client instance.
    """

    def __init__(self, config: Config):
        self.config: Config = config
        self._client: MongoClient = MongoClient(self.config.get('mongo_uri'), serverSelectionTimeoutMS=5000)
        self._check_mongo_availability()

    def _check_mongo_availability(self) -> None:
        """
        Check if the MongoDB server is available. Raise an error if it's not.

        Raises:
            ConnectionError: If there's a failure in connecting to MongoDB.
        """
        try:
            self._client.server_info()
        except mongo_errors.ServerSelectionTimeoutError:
            raise ConnectionError("Cannot connect to MongoDB.")

    def insert_records(self, records: Dict[str, Any], file_path: str, sheet_name: str) -> None:
        """
        Insert or update records into MongoDB.

        Args:
            records (Dict[str, Any]): The records to insert/update.
            file_path (str): The source file of the records.
            sheet_name (str): The name of the Excel sheet containing the records.
        """
        db_collection = self._client[self.config.get('database_name')][self.config.get('collection_name')]

        if "metadata" not in records or "records" not in records:
            logger.warning(f"Invalid records format in file {file_path}. Skipping insertion for sheet {sheet_name}.")
            return

        record_id = hashlib.md5(sheet_name.encode()).hexdigest()
        db_collection.update_one({"_id": record_id}, {"$set": records}, upsert=True)

        logger.info(f"Successfully inserted/updated records from {file_path} for sheet {sheet_name} into MongoDB.")

    def _retry_insert(self, valid_records, file_path, db_collection):
        for retry in range(self.config.get('max_retries')):
            try:
                db_collection.insert_many(valid_records)
                logger.info(f"Successfully inserted {len(valid_records)} records from {file_path} into MongoDB.")
                return
            except (mongo_errors.ServerSelectionTimeoutError, mongo_errors.OperationFailure) as e:
                self._handle_database_error(e, file_path, retry)
            except Exception as e:
                logger.error(f"Unexpected error inserting records from {file_path}: {e}")
                break

    @staticmethod
    def _handle_database_error(error, file_path, retry_count):
        if isinstance(error, (mongo_errors.ServerSelectionTimeoutError, mongo_errors.OperationFailure)):
            time.sleep(3 * (retry_count + 1))
            logger.warning(f"Database error for {file_path}. Retrying...")


class XLSXProcessor:
    """
    A class to process and transform Excel files.

    Attributes:
        config (Config): An instance of the Config class to get configuration values.
        mongo_handler (MongoDBHandler): An instance of the MongoDBHandler class to perform MongoDB operations.
    """

    def __init__(self, config: Config, mongo_handler: MongoDBHandler):
        self.config: Config = config
        self.mongo_handler: MongoDBHandler = mongo_handler

    def process_file(self, file_path: str) -> None:
        """
        Process the Excel file and insert/update its records into MongoDB.

        Args:
            file_path (str): The path to the Excel file.
        """
        data = self._convert_excel_to_json(file_path)
        for sheet_name, sheet_data in data.items():
            self.mongo_handler.insert_records(sheet_data, file_path, sheet_name)

    def _convert_excel_to_json(self, input_excel_path):
        def make_unique_columns(cols):
            seen = {}
            for idx, col in enumerate(cols):
                original_col = col
                counter = 1
                while col in seen:
                    col = f"{original_col}_{counter}"
                    counter += 1
                seen[col] = True
                cols[idx] = col
            return cols

        try:
            # Read the entire excel without skipping headers
            entire_workbook = pd.read_excel(input_excel_path, sheet_name=None, header=None, engine='openpyxl')

            result_data = {}
            for sheet_name, entire_data_frame in entire_workbook.items():
                # Skip sheets with the name 'Contents'
                if sheet_name == 'Contents':
                    continue

                # Extract metadata
                metadata = {
                    "header": entire_data_frame.iloc[0, 1],
                    "metadata": entire_data_frame.iloc[1, 0],
                    "table_info": entire_data_frame.iloc[2, 0],
                    "copyright": entire_data_frame.iloc[-1, 0],
                }

                # Now read the actual data, skipping the first 4 rows for the data section
                data_frame = entire_data_frame[4:].reset_index(drop=True)

                # Convert column names to string representation
                columns = data_frame.iloc[0].astype(str).tolist()

                # Make the column names unique
                unique_columns = make_unique_columns(columns)
                data_frame.columns = unique_columns

                # skip the first row as it contains column names, and the second as it contains redundant data
                data_frame = data_frame[2:].reset_index(drop=True)  # Use the first data row as headers

                records = self._data_frame_to_json(data_frame)

                result_data[sheet_name] = {
                    "metadata": metadata,
                    "records": records
                }

            return result_data
        except Exception as unexpected_reading_error:
            logger.error(f"Unexpected error reading {input_excel_path}: {unexpected_reading_error}")
            return {}

    @staticmethod
    def _data_frame_to_json(data_frame):
        # Ensure there are at least two columns.
        if len(data_frame.columns) < 2:
            logger.warning("DataFrame has fewer than two columns. Unable to create key-value pairs.")
            return data_frame.to_dict(orient='records')

        # Extract the first two columns.
        key_col = data_frame.columns[0]
        value_col = data_frame.columns[1]

        records = []
        for _, row in data_frame.iterrows():
            record = {}

            # Add the key-value pair from the first two columns.
            key_value = row[key_col]
            value_value = row[value_col]
            if pd.notna(key_value) and pd.notna(value_value):
                record[str(key_value)] = value_value

            # Add the remaining columns.
            for col in data_frame.columns[2:]:
                record[str(col)] = row[col] if pd.notna(row[col]) else ""

            records.append(record)

        return records

    @staticmethod
    def _filter_data(data_frame):
        return data_frame[data_frame[data_frame.columns[0]].apply(
            lambda x: isinstance(x, (int, float))) & data_frame[data_frame.columns[1]].notna()]


class XLSXHandler(FileSystemEventHandler):
    """
    A class to handle file system events related to Excel files.

    Attributes:
        processor (XLSXProcessor): An instance of the XLSXProcessor class to process Excel files.
        _file_lock (multiprocessing.Lock): A lock to handle concurrent file events.
        _last_processed_times (Dict[str, float]): A dictionary to track the last processed time for each file.
        queue (multiprocessing.Queue): A queue to add files for processing.
    """

    def __init__(self, queue: multiprocessing.Queue, processor: XLSXProcessor):
        super().__init__()
        self.processor: XLSXProcessor = processor
        self._file_lock: multiprocessing.Lock = multiprocessing.Lock()
        self._last_processed_times: Dict[str, float] = {}
        self.queue: multiprocessing.Queue = queue

    def on_modified(self, event):
        if self._is_valid_event(event):
            self.queue.put(event.src_path)

    def _is_valid_event(self, event):
        return (self._is_xlsx_file(event)
                and self._can_process_file(event.src_path)
                and self._is_file_stable(event.src_path))

    @staticmethod
    def _is_xlsx_file(event):
        return not event.is_directory and event.src_path.endswith('.xlsx')

    def _can_process_file(self, src_path):
        current_time = time.time()
        with self._file_lock:
            last_processed_time = self._last_processed_times.get(src_path, 0)
            if current_time - last_processed_time < 300:
                return False
            self._last_processed_times[src_path] = current_time
            return True

    def _is_file_stable(self, src_path, wait_time=5):
        initial_hash = compute_sha256(src_path, self.processor.config)
        time.sleep(wait_time)
        return initial_hash == compute_sha256(src_path, self.processor.config)


def compute_sha256(file_path, config):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(config.get('buffer_size')), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def worker(queue, config):
    mongo_handler = MongoDBHandler(config)
    processor = XLSXProcessor(config, mongo_handler)
    while True:
        file_path = queue.get()
        if file_path is None:
            break
        processor.process_file(file_path)


def start_workers(queued_files, config):
    return [multiprocessing.Process(target=worker, args=(queued_files, config)) for _ in
            range(config.get('num_processes'))]


def get_all_xlsx_files_in_directory(directory_path):
    return [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.xlsx')]


def initialize_and_start_file_observer(watch_directory, queue, processor):
    event_handler = XLSXHandler(queue, processor)
    observer_instance = Observer()
    observer_instance.schedule(event_handler, watch_directory, recursive=True)
    observer_instance.start()
    return observer_instance


def check_dir_exists(dir_path):
    if not os.path.exists(dir_path):
        logger.error(f"The directory {dir_path} does not exist. Exiting program.")
        sys.exit(1)


def starting_message(config):
    logger.info("Press Ctrl+C to stop the program.")
    logger.info(f"Watching directory: {config.get('watch_directory')}")
    logger.info(f"Number of processes: {config.get('num_processes')}")
    logger.info(f"MongoDB URI: {config.get('mongo_uri')}")
    logger.info(f"MongoDB database name: {config.get('database_name')}")
    logger.info(f"MongoDB collection name: {config.get('collection_name')}")


def handle_interrupt(config, process_pool, queue, observer_instance):
    try:
        starting_message(config)
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected. Waiting for ongoing tasks to complete before shutting down...")
        for _ in range(config.get('num_processes')):
            queue.put(None)
        for proc in process_pool:
            proc.join()
        observer_instance.stop()
    finally:
        observer_instance.join()


def start() -> None:
    """
    The main function to start the program. It initializes the necessary components, starts worker processes,
    sets up a file observer, and handles interruptions gracefully.
    """
    configuration = Config()
    mongo_handler = MongoDBHandler(configuration)
    processor = XLSXProcessor(configuration, mongo_handler)
    queued_files = multiprocessing.Queue()
    check_dir_exists(configuration.get('watch_directory'))
    for file_path in get_all_xlsx_files_in_directory(configuration.get('watch_directory')):
        queued_files.put(file_path)
    process_pool = start_workers(queued_files, configuration)
    for proc in process_pool:
        proc.start()
    observer_instance = initialize_and_start_file_observer(configuration.get('watch_directory'), queued_files,
                                                           processor)
    handle_interrupt(configuration, process_pool, queued_files, observer_instance)


if __name__ == '__main__':
    start()
