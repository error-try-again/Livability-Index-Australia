#!/bin/bash
set -e

# Define the Python script filename
PYTHON_SCRAPER_FILENAME="web_scraper.py"

# Function to check if a command is available
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo "Error: $1 is not installed or not available in PATH."
        exit 1
    fi
}

# Check if pip3 and python3 are installed and available
check_command pip3
check_command python3

# Function to check and install required Python packages
check_and_install_python_packages() {
    installed_packages=$(pip3 freeze)
    for package in "$@"; do
        if echo "$installed_packages" | grep -q "^$package="; then
            echo "$package is already installed."
        else
            echo "Installing Python package: $package"
            pip3 install $package
        fi
    done
}

# Trap function to handle user interrupts
handle_interrupt() {
    echo -e "\nUser interrupt detected. Cleaning up..."
    if [[ -f $PYTHON_SCRAPER_FILENAME ]]; then
        rm $PYTHON_SCRAPER_FILENAME
    fi
    exit 1
}

# Set the trap for SIGINT
trap handle_interrupt SIGINT

# Function to create urls.json with a list of URLs
create_urls_json() {
    cat <<EOL > urls.json
[
    "https://www.abs.gov.au"
]
EOL
}

# Function to write the Python scraper to a file
write_python_scraper_to_file() {
    cat <<EOL > $PYTHON_SCRAPER_FILENAME
import os
import time
import json
import requests
import logging
import logging.handlers
import hashlib
import threading
import queue
import datetime
import gc
import tracemalloc
import signal
from collections import deque
from random import randint
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from robotexclusionrulesparser import RobotExclusionRulesParser

# Configure logging settings
log_formatter = logging.Formatter('{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}')
log_file_handler = logging.handlers.RotatingFileHandler('web_scraper.log', maxBytes=10 * 1024 * 1024,
                                                        backupCount=5)  # 10MB per file with 5 backups
log_file_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(log_file_handler)
logging.getLogger().setLevel(logging.INFO)

# Configure console logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(console_handler)

# Configure memory profiling logging
memory_log = logging.getLogger('memory_profiler')
memory_log.setLevel(logging.INFO)
memory_handler = logging.FileHandler('memory_profiler.log', mode='w')
memory_log.addHandler(memory_handler)


def signal_handler(signum, frame):
    """Handle the SIGINT signal."""
    logging.info("SIGINT received. Preparing to stop the script gracefully...")
    scraper.stop_signal_received = True

    # Memory profiling on exit
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')
    for stat in top_stats[:10]:
        memory_log.info(stat)


def normalize_url(url):
    """Normalize the given URL."""
    parsed = urlparse(url)
    # Remove URL fragment and sort query parameters
    normalized = parsed._replace(fragment="", query="&".join(sorted(parsed.query.split("&"))))
    # Convert to lowercase and remove trailing slash
    return urlunparse(normalized).rstrip("/").lower()


def print_logs(num_lines=5):
    """
    Reads the most recent lines from the log file and prints them to the console.

    Args:
    - num_lines (int): Number of recent lines to print. Default is 5.
    """
    log_file_path = 'web_scraper.log'
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as log_file:
            lines = log_file.readlines()
            for line in lines[-num_lines:]:
                print(line.strip())
    else:
        print("Log file not found.")


def compute_md5(filename):
    """Compute the MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class WebScraper:
    def __init__(self, user_agent, delay=2, download_folder="downloaded_files", resume=False):
        # Initializer for the WebScraper class
        self.USER_AGENT = user_agent  # User agent for the web requests
        self.DELAY = delay  # Delay between requests
        self.DOWNLOAD_FOLDER = download_folder  # Directory to save downloaded files
        self.visited_domains = set()  # Set to keep track of visited domains
        self.robots_parser = RobotExclusionRulesParser()  # Parser for robots.txt
        self.start_time = None  # Time when the scraping started
        self.pages_crawled = 0  # Counter for pages crawled
        self.lock = threading.Lock()  # Lock for thread safety
        self.stop_signal_received = False  # Flag to check if stop signal is received
        self.resume = resume  # Flag to check if the script should resume from a backup

        # Initialize visited_links from backup_state.json
        if os.path.exists("backup_state.json"):
            with open("backup_state.json", "r") as f:
                state = json.load(f)
                self.visited_links = set(state["visited_links"])
        else:
            self.visited_links = set()

    def worker(self, url_queue):
        excluded_directories = ["calendar", "homepage_release_calendar", "about"]
        excluded_extensions = ['.zip', '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.png', '.jpg', '.jpeg', '.gif', '.csv', '.xml', '.json', '.txt']

        # Worker function for threads
        while not self.stop_signal_received:
            try:
                url = url_queue.get_nowait()
            except queue.Empty:
                break
            self.crawl_sublinks(url, urlparse(url).netloc, excluded_directories, excluded_extensions)
            url_queue.task_done()

    def backup_state(self):
        """Backup the current state to a file."""
        with open("backup_state.json", "w") as f:
            state = {
                "visited_links": list(self.visited_links),
            }
            json.dump(state, f)

    def load_state(self):
        """Load the state from a backup file."""
        if os.path.exists("backup_state.json"):
            with open("backup_state.json", "r") as f:
                state = json.load(f)
                self.visited_links = set(state["visited_links"])

    def estimate_time_remaining(self, total_links):
        # Estimate the time remaining for the scraping process
        if not self.start_time:
            return "Estimation not available yet."

        elapsed_time = time.time() - self.start_time
        average_time_per_page = elapsed_time / self.pages_crawled if self.pages_crawled else 0
        estimated_remaining_time = average_time_per_page * (total_links - self.pages_crawled)

        # Convert estimated time from seconds to a more readable format
        estimated_time_str = str(datetime.timedelta(seconds=int(estimated_remaining_time)))

        return f"Crawled {self.pages_crawled} out of an estimated {total_links} pages. Estimated time remaining: {estimated_time_str}."

    # Fetch data from a given URL and handle rate limits and retries with exponential backoff
    def fetch_data_from_url(self, url, retries=3, backoff_in_seconds=10):
        headers = {"User-Agent": self.USER_AGENT}
        start_time = time.time()
        logging.debug(f"Starting request for URL: {url}")
        try:
            response = requests.get(url, headers=headers)
            elapsed_time = time.time() - start_time

            # Handle 429 Status Code with exponential backoff
            if response.status_code == 429 and retries > 0:
                wait_time = backoff_in_seconds * (2 ** (3 - retries)) + randint(0,
                                                                                10)  # Exponential backoff with jitter
                logging.warning(f"Rate limit detected for URL {url}. Pausing for {wait_time} seconds.")
                time.sleep(wait_time)
                logging.info(f"Resuming after rate limit wait for URL {url}.")
                return self.fetch_data_from_url(url, retries - 1)

            response.raise_for_status()
            logging.info(f"Fetched data from {url} in {elapsed_time:.2f} seconds")

            # Monitor Server Response Times
            if elapsed_time > 5:  # Assuming 5 seconds is a threshold for a slow response
                logging.warning(f"Slow response from {url}. Consider reducing the request rate.")
                self.DELAY += 1  # Increase delay by 1 second

            logging.debug(f"Finished request for URL: {url}")
            return response.text
        except requests.RequestException as e:
            if retries > 0:
                logging.warning(f"Error fetching data from {url}. Retrying... Remaining retries: {retries - 1}")
                time.sleep(backoff_in_seconds)
                return self.fetch_data_from_url(url, retries - 1)
            logging.error(f"Error fetching data from {url}: {e}")
            return ""

    def extract_xlsx_links(self, html_content, base_url):
        # Extract .xlsx links from the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')
        return [urljoin(base_url, a['href']) for a in soup.find_all('a', href=True)
                if a['href'].endswith('.xlsx') and self.robots_parser.is_allowed(self.USER_AGENT, a['href'])]

    def download_file(self, url, force_download=False):
        local_filename = os.path.join(self.DOWNLOAD_FOLDER, url.split('/')[-1])
        if not os.path.exists(self.DOWNLOAD_FOLDER):
            os.makedirs(self.DOWNLOAD_FOLDER)
            logging.info(f"Created download directory: {self.DOWNLOAD_FOLDER}")
        if os.path.exists(local_filename) and not force_download:
            response = requests.head(url)
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
        try:
            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            logging.info(f"Downloaded {url} to {local_filename}")
        except requests.RequestException as e:
            logging.error(f"Error downloading {url}: {e}")
        except Exception as e:
            # Handle other potential errors during file download
            logging.error(f"Unexpected error during file download: {e}")

    def crawl_sublinks(self, start_url, domain, excluded_directories, excluded_extensions):
        # Initialize the start time if it hasn't been set
        if not self.start_time:
            self.start_time = time.time()

        # Use a deque for Breadth-First Search traversal
        bfs_queue = deque([start_url])
        total_links = len(bfs_queue)
        counter = 0  # Counter to decide when to clear the console

        while bfs_queue and not self.stop_signal_received:
            url = bfs_queue.popleft()

            # Using the lock when checking and updating shared resources
            with self.lock:
                if url in self.visited_links:
                    logging.info(f"URL {url} has already been visited. Skipping.")
                    continue
                self.visited_links.add(url)

            html_content = self.fetch_data_from_url(url)

            self.pages_crawled += 1
            total_links = max(total_links,
                              len(bfs_queue) + self.pages_crawled)  # Update total_links with the maximum seen so far
            logging.info(self.estimate_time_remaining(total_links))

            try:
                soup = BeautifulSoup(html_content, 'lxml')
            except Exception as e:
                logging.error(f"Error parsing content from {url}: {e}")
                continue

            for a in soup.find_all('a', href=True):
                link = normalize_url(urljoin(url, a['href']))
                parsed_link = urlparse(link)

                if parsed_link.netloc == domain:
                    with self.lock:
                        if link not in self.visited_links:
                            # Check for redundant URL patterns, excluded directories, and excluded extensions
                            if (
                                    not any(excluded_dir in link for excluded_dir in excluded_directories)
                                    and not any(link.endswith(ext) for ext in excluded_extensions)
                            ):
                                if link.endswith('.xlsx') and self.robots_parser.is_allowed(self.USER_AGENT, link):
                                    self.download_file(link)
                                else:
                                    bfs_queue.append(link)

            time.sleep(self.DELAY)

            # Increment the counter and clear the console if needed
            counter += 1
            if counter % 50 == 0:  # Clear the console after processing every 50 URLs
                os.system('clear' if os.name == 'posix' else 'cls')

        # If the stop signal is received, backup the current state
        if self.stop_signal_received:
            self.backup_state()

        # Clean up resources
        del bfs_queue
        gc.collect()

    def main(self, urls):
        # Backup and Resume
        if self.resume:
            self.load_state()  # Load the state if a backup exists
            urls = [url for url in urls if url not in self.visited_links]  # Filter out visited URLs

        # Main function to start the scraping process
        for url in urls:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            if domain not in self.visited_domains:
                robots_url = urljoin(url, "/robots.txt")
                logging.debug(f"Fetching robots.txt from {robots_url}")
                self.robots_parser.fetch(robots_url)
                if self.robots_parser.get_crawl_delay("*"):
                    self.DELAY = max(self.DELAY, self.robots_parser.get_crawl_delay("*"))
                self.visited_domains.add(domain)
                time.sleep(self.DELAY)

        url_queue = queue.Queue()
        for url in urls:
            url_queue.put(url)

        # Start threads for each URL
        threads = []
        for _ in range(len(urls)):
            thread = threading.Thread(target=self.worker, args=(url_queue,))
            thread.start()
            threads.append(thread)

        # Wait for all threads to finish
        for thread in threads:
            thread.join()
            logging.debug(f"Thread {thread.name} has finished.")

        # Backup visited links after scraping
        self.backup_state()

        logging.info(
            f"Visited links: {len(self.visited_links)}, Downloaded files: {len(os.listdir(self.DOWNLOAD_FOLDER))}")


if __name__ == "__main__":
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    USER_AGENT = DEFAULT_USER_AGENT
    NUM_THREADS = int(os.cpu_count() * 0.10)  # Use 10% of the available CPU cores for the scraper
    logging.info(f"Using User-Agent: {USER_AGENT}")
    logging.info(f"Using {NUM_THREADS} threads for scraping.")

    try:
        with open('urls.json', 'r') as f:
            urls = json.load(f)
            if not urls:
                logging.error("The urls.json file is empty. Exiting.")
                exit(1)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error reading or parsing urls.json: {e}")
        print("Please ensure that the urls.json file exists and is correctly formatted.")
        exit(1)

    tracemalloc.start()  # Start memory profiling

    # Set up signal handling
    signal.signal(signal.SIGINT, signal_handler)

    if os.path.exists("backup_state.json"):
        # Determine if the scraper should resume from a previous state
        resume_scraper = input("Do you want to resume from a previous state? (yes/no): ").strip().lower() == "yes"
    else:
        resume_scraper = False

    if not resume_scraper:
        # Remove the backup file if the scraper is not resuming from a previous state
        if os.path.exists("backup_state.json"):
            os.remove("backup_state.json")

    # Initialize the scraper instance
    scraper = WebScraper(user_agent=USER_AGENT, resume=resume_scraper)

    # Set up signal handling with the scraper instance
    signal.signal(signal.SIGINT, signal_handler)

    scraper.main(urls)

    print("\nMost recent logs from the web scraper:")
    print_logs()

EOL
      echo "Python scraper written to $PYTHON_SCRAPER_FILENAME"
}

# Create urls.json with a list of URLs
create_urls_json

# Check and install required packages
check_and_install_python_packages requests beautifulsoup4 robotexclusionrulesparser lxml

# Write the Python scraper script
write_python_scraper_to_file

# Execute the Python scraper
python3 $PYTHON_SCRAPER_FILENAME

# Remove the temporary Python script after execution
rm $PYTHON_SCRAPER_FILENAME

echo "Done!"
