import hashlib
import os
import json
import sys
import time
import requests
import logging
import logging.handlers
import threading
import datetime
import tracemalloc
import signal
from collections import deque, OrderedDict
from urllib.parse import urljoin, urlparse, urlunparse
from bs4 import BeautifulSoup
from robotexclusionrulesparser import RobotExclusionRulesParser

log_file_formatter = logging.Formatter(
    '{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}')
log_console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

log_file_handler = logging.handlers.RotatingFileHandler('web_scraper.log', maxBytes=10 * 1024 * 1024, backupCount=5)
log_file_handler.setFormatter(log_file_formatter)
log_file_handler.setLevel(logging.DEBUG)  # Set file handler level to DEBUG

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_console_formatter)
console_handler.setLevel(logging.DEBUG)  # Set console handler level to DEBUG

logging.basicConfig(level=logging.DEBUG, handlers=[log_file_handler, console_handler])  # Set global level to DEBUG

memory_log = logging.getLogger('memory_profiler')
memory_log.setLevel(logging.INFO)
memory_handler = logging.FileHandler('memory_profiler.log', mode='w')
memory_log.addHandler(memory_handler)


def normalize_url(normalizable_url):
    """Normalize the given URL."""
    parsed = urlparse(normalizable_url)
    sorted_query = "&".join(sorted(parsed.query.split("&")))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, sorted_query, "")).rstrip("/").lower()


def is_same_domain(start_url, url_to_check):
    """Check if the given url is in the same domain as the start_url."""
    return urlparse(start_url).netloc == urlparse(url_to_check).netloc


def load_config():
    """Load the configuration from the JSON file."""
    try:
        with open('config.json', 'r') as config_file:
            return json.load(config_file)
    except Exception as load_error:
        logging.error(f"Error reading configuration: {load_error}")
        raise


config = load_config()
config['excluded_directories'] = set(config.get('excluded_directories', []))
config['excluded_extensions'] = set(config.get('excluded_extensions', []))

worker_stop_event = threading.Event()


class LRUCache:
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity
        self.lock = threading.Lock()

    def get(self, key: str):
        with self.lock:
            if key not in self.cache:
                return None
            self.cache.move_to_end(key)
            return self.cache[key]

    def set(self, key: str) -> None:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                if len(self.cache) >= self.capacity:
                    self.cache.popitem(last=False)
                self.cache[key] = True

    def bulk_add(self, keys: list) -> None:
        """Add multiple keys to the cache."""
        for key in keys:
            self.set(key)

    def __contains__(self, key: str) -> bool:
        with self.lock:
            return key in self.cache


def log_memory_profiling():
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')
    for stat in top_stats[:10]:
        memory_log.info(stat)
    tracemalloc.stop()


def verify_backup_state():
    backup_filename = "backup_state.json"

    if not os.path.exists(backup_filename):
        logging.error(f"{backup_filename} does not exist!")
        return False

    try:
        with open(backup_filename, "r") as file:
            data = json.load(file)

            if "visited_links" not in data or "file_hashes" not in data:
                logging.error(f"{backup_filename} is missing expected keys!")
                return False

            if not data["visited_links"] or not isinstance(data["visited_links"], list):
                logging.error("visited_links in backup_state.json is empty or not a list!")
                return False

            if len(data["visited_links"]) != len(set(data["visited_links"])):
                logging.error("visited_links in backup_state.json contains duplicate entries!")
                return False

    except json.JSONDecodeError:
        logging.error(f"{backup_filename} is not a valid JSON!")
        return False
    except Exception as decode_error:
        logging.error(f"Error verifying {backup_filename}: {decode_error}")
        return False

    return True


def compute_file_hash(file_path):
    """Compute the SHA-256 hash of a file."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def compute_url_hash(url):
    """Compute the SHA-256 hash of a URL string."""
    return hashlib.sha256(url.encode()).hexdigest()


class WebScraper:
    def __init__(self, user_agent, starting_urls=None, resume=False, delay=3, download_folder="downloaded_files"):
        if starting_urls is None:
            starting_urls = []
        self.USER_AGENT = user_agent
        self.DELAY = delay
        self.DOWNLOAD_FOLDER = "downloaded_files"
        self.visited_domains = set()
        self.robots_parser = RobotExclusionRulesParser()
        self.start_time = None
        self.pages_crawled = 0
        self.lock = threading.Lock()
        self.stop_signal_received = False
        self.resume = resume
        self.starting_urls = starting_urls
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.USER_AGENT})
        self.file_hashes = {}
        self.threads = []
        self.visited_links = LRUCache(10000)
        self.robots_parsers = {}

        # Ensure the download directory exists
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def start_scraper(self, num_threads):
        tracemalloc.start()

        url_deque = deque()

        # Add the starting URL
        for url in self.starting_urls:
            url_deque.append(url)

        # If resuming, load the saved state
        if self.resume:
            pending_urls_from_backup = self.load_state()
            url_deque.extend(pending_urls_from_backup)

        new_urls = []

        for each_url in url_deque:
            normalized_url = normalize_url(each_url)
            if self.resume and normalized_url in self.visited_links:
                continue
            new_urls.append(normalized_url)

        # Add the new URLs to the deque outside the loop to avoid mutating the deque while iterating over it
        url_deque.extend(new_urls)

        try:
            # Start worker threads
            for _ in range(num_threads):
                thread = threading.Thread(target=self.worker, args=(url_deque,))
                thread.start()
                self.threads.append(thread)
        finally:
            for thread in self.threads:
                thread.join()

    def signal_handler(self, _, __):
        worker_stop_event.set()

        logging.info("Stopping scraper. Waiting for threads to finish.")
        self.stop_signal_received = True

        for thread in self.threads:
            thread.join()

    def can_fetch(self, fetchable_url):
        domain = urlparse(fetchable_url).netloc
        if domain not in self.robots_parsers:
            try:
                rp_url = urljoin(fetchable_url, '/robots.txt')
                rp = self.session.get(rp_url)
                parser = RobotExclusionRulesParser()
                parser.parse(rp.text)
                self.robots_parsers[domain] = parser
            except Exception as robots_fetch_error:
                logging.warning(f"Error fetching robots.txt for {domain}: {robots_fetch_error}")
                return False

        if not self.robots_parsers[domain].is_allowed(self.USER_AGENT, fetchable_url):
            logging.warning(f"URL {fetchable_url} is disallowed by robots.txt")
            return False

        return True

    def worker(self, url_deque):
        logging.info(f"Starting worker thread {threading.current_thread().name}")

        while not worker_stop_event.is_set():
            try:
                try:
                    logging.debug(f"Queue size: {len(url_deque)}")
                    queued_url = url_deque.popleft()
                except IndexError:  # No more URLs in the queue
                    logging.info(f"Queue is empty. Worker thread {threading.current_thread().name} exiting.")
                    break

                # Check if the URLs hash is in the visited_links cache.
                url_hash = compute_url_hash(queued_url)
                with self.lock:
                    if url_hash in self.visited_links:
                        logging.debug(f"URL {queued_url} is already visited. Skipping.")
                        continue
                    self.visited_links.set(url_hash)

                url_hash = compute_url_hash(queued_url)
                self.file_hashes[url_hash] = queued_url

                file_extension = os.path.splitext(urlparse(queued_url).path)[1]
                if file_extension in config['excluded_extensions']:
                    continue

                if not self.can_fetch(queued_url):
                    continue

                if not any(dir_name in queued_url for dir_name in config['excluded_directories']):
                    response = self.fetch_data_from_url(queued_url)
                    if response and response.status_code == 200:
                        if file_extension in ['.xlsx']:
                            file_extension = "xlsx"
                            self.download_file(queued_url, file_extension)
                        elif file_extension in ['.zip']:
                            file_extension = "zip"
                            self.download_file(queued_url, file_extension)

                    time.sleep(self.DELAY)

                    if response:
                        mime_type = response.headers.get('content-type', '').split(';')[0]
                        if mime_type not in ['text/html', 'application/xhtml+xml']:
                            continue

                        content = response.content
                        soup = BeautifulSoup(content, 'lxml')
                        new_urls = []

                        for link in soup.find_all('a', href=True):
                            absolute_url = urljoin(queued_url, link['href'])
                            normalized_url = normalize_url(absolute_url)
                            if any(is_same_domain(start_url, normalized_url) for start_url in self.starting_urls):
                                if not self.visited_links.get(normalized_url):
                                    new_urls.append(normalized_url)

                        url_deque.extend(new_urls)

                    with self.lock:
                        self.pages_crawled += 1

                    if self.pages_crawled % 10 == 0:
                        logging.debug(f"Pages crawled: {self.pages_crawled}. Backing up state.")
                        self.backup_state(url_deque)

            except Exception as thread_error:
                logging.error(f"Error in worker thread {threading.current_thread().name}: {thread_error}")

    def backup_state(self, url_deque):
        with self.lock:
            backup_filename = f"backup_state.json"
            try:
                with open(backup_filename, "w") as temp_state_file:
                    state = {
                        "visited_links": list(self.visited_links.cache.keys()),
                        "file_hashes": self.file_hashes,
                        "pending_links": list(url_deque)  # Save the remaining URLs in the deque
                    }
                    json.dump(state, temp_state_file)
                logging.info(f"Backup saved as {backup_filename}")
            except Exception as backup_error:
                logging.error(f"Error during backup: {backup_error}")

    def load_state(self):
        with self.lock:
            if not verify_backup_state():
                logging.warning("Backup verification failed!")
                return []

            backups = [f for f in os.listdir() if f.startswith("backup_state")]

            if not backups:
                logging.warning("No backup state files found.")
                return False

            latest_backup = sorted(backups, reverse=True)[0]
            logging.info(f"Loading from the latest backup: {latest_backup}")

            pending_urls = []
            try:
                with open(latest_backup, "r") as backup_json:
                    state = json.load(backup_json)

                    # Populate LRUCache for visited links
                    self.visited_links.bulk_add(state["visited_links"])

                    # Populate file_hashes data structure
                    self.file_hashes = state["file_hashes"]

                    logging.info(f"Loaded {len(state['visited_links'])} visited links from backup.")
                    logging.info(f"Loaded {len(self.file_hashes)} file hashes from backup.")

                    # Load the pending URLs into a local list
                    pending_urls = state.get("pending_links", [])

            except Exception as load_backup_error:
                logging.error(f"Error loading from backup {latest_backup}: {load_backup_error}")

        return pending_urls

    def fetch_data_from_url(self, queued_url, retries=3):
        for attempt in range(retries):
            try:
                response = self.session.get(queued_url, timeout=10)
                response.raise_for_status()
                return response
            except requests.RequestException as fetch_error:
                logging.warning(f"Failed to fetch data from {queued_url}. Error: {fetch_error}. Retrying...")
        logging.error(f"Failed to fetch data from {queued_url} after {retries} retries.")
        return None

    def download_file(self, queued_url, file_extension, force_download=False):
        local_filename = os.path.join(self.DOWNLOAD_FOLDER + "/" + file_extension,
                                      queued_url.split('/')[-1])

        if not os.path.exists(self.DOWNLOAD_FOLDER + "/" + file_extension):
            os.makedirs(self.DOWNLOAD_FOLDER + "/" + file_extension)
            logging.info(f"Created download directory: {self.DOWNLOAD_FOLDER + '/' + file_extension}")

        # If the file already exists locally
        if os.path.exists(local_filename):
            existing_hash = compute_file_hash(local_filename)

            # If the hash of the existing file matches the previously stored hash, skip the download
            if queued_url in self.file_hashes and self.file_hashes[queued_url] == existing_hash and not force_download:
                logging.info(f"File {local_filename} already exists with matching hash. Skipping download.")
                return
            elif not force_download:
                response = requests.head(queued_url)
                if 'Last-Modified' in response.headers:
                    file_time = os.path.getmtime(local_filename)
                    server_time = time.mktime(datetime.datetime.strptime(response.headers['Last-Modified'],
                                                                         '%a, %d %b %Y %H:%M:%S GMT').timetuple())
                    if server_time <= file_time:
                        logging.info(f"File {local_filename} already exists and is up-to-date. Skipping download.")
                        return
                else:
                    logging.info(f"File {local_filename} already exists. Skipping download.")
                    return

        # Continue with the download if above conditions are not met
        try:
            with requests.get(queued_url, stream=True) as response:
                logging.info(f"Downloading {queued_url} to {local_filename}")
                response.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                # Once the file is downloaded, compute its hash and store in the file_hashes dictionary
                file_hash = compute_file_hash(local_filename)
                self.file_hashes[queued_url] = file_hash

                logging.info(f"Downloaded {queued_url} to {local_filename} with hash {file_hash}")
        except requests.RequestException as download_error:
            logging.error(f"Error downloading {queued_url}: {download_error}")
        except Exception as other_network_error:
            # Handle other potential errors during file download
            logging.error(f"Unexpected error during file download: {other_network_error}")


def read_json_urls():
    try:
        with open('urls.json', 'r') as json_file:
            extracted_urls = json.load(json_file)
            logging.info(f"Loaded {len(extracted_urls)} URLs from urls.json.")
            return extracted_urls
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error reading urls.json: {e}")
        sys.exit(1)


if __name__ == '__main__':
    USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    NUM_THREADS = 1

    url_list = read_json_urls()

    web_scraper = WebScraper(USER_AGENT, starting_urls=url_list, resume=True, delay=3)

    web_scraper.start_scraper(NUM_THREADS)
