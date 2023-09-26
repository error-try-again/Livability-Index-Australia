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
import hashlib
import threading
import queue
import datetime
from random import randint
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from robotexclusionrulesparser import RobotExclusionRulesParser
from collections import deque

# Configure logging settings
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='web_scraper.log', filemode='w')

class WebScraper:
    def __init__(self, user_agent, delay=2, download_folder="downloaded_files"):
        # Initializer for the WebScraper class
        self.USER_AGENT = user_agent  # User agent for the web requests
        self.DELAY = delay  # Delay between requests
        self.DOWNLOAD_FOLDER = download_folder  # Directory to save downloaded files
        self.visited_links = set()  # Set to keep track of visited links
        self.visited_domains = set()  # Set to keep track of visited domains
        self.robots_parser = RobotExclusionRulesParser()  # Parser for robots.txt
        self.start_time = None  # Time when the scraping started
        self.pages_crawled = 0  # Counter for pages crawled
        self.lock = threading.Lock()  # Lock for thread safety

    def print_logs(self, num_lines=5):
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

    def worker(self, url_queue):
        # Worker function for threads
        while not url_queue.empty():
            try:
                url = url_queue.get_nowait()
            except queue.Empty:
                break
            self.crawl_sublinks(url, urlparse(url).netloc)
            url_queue.task_done()

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

    def fetch_data_from_url(self, url, retries=3, backoff_in_seconds=10):
        headers = {"User-Agent": self.USER_AGENT}
        start_time = time.time()
        try:
            response = requests.get(url, headers=headers)
            elapsed_time = time.time() - start_time

            # Handle 429 Status Code with exponential backoff
            if response.status_code == 429 and retries > 0:
                wait_time = backoff_in_seconds * (2 ** (3 - retries)) + randint(0, 10)  # Exponential backoff with jitter
                logging.warning(f"Rate limit detected for URL {url}. Pausing for {wait_time} seconds.")
                time.sleep(wait_time)
                logging.info(f"Resuming after rate limit wait for URL {url}.")
                return self.fetch_data_from_url(url, retries-1)

            response.raise_for_status()
            logging.info(f"Fetched data from {url} in {elapsed_time:.2f} seconds")

            # Monitor Server Response Times
            if elapsed_time > 5:  # Assuming 5 seconds is a threshold for a slow response
                logging.warning(f"Slow response from {url}. Consider reducing the request rate.")
                self.DELAY += 1  # Increase delay by 1 second

            return response.text
        except requests.RequestException as e:
            if retries > 0:
                logging.warning(f"Error fetching data from {url}. Retrying... Remaining retries: {retries-1}")
                time.sleep(backoff_in_seconds)
                return self.fetch_data_from_url(url, retries-1)
            logging.error(f"Error fetching data from {url}: {e}")
            return ""

    def extract_xlsx_links(self, html_content, base_url):
        # Extract .xlsx links from the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')
        return [urljoin(base_url, a['href']) for a in soup.find_all('a', href=True)
                if a['href'].endswith('.xlsx') and self.robots_parser.is_allowed(self.USER_AGENT, a['href'])]

    def compute_md5(self, filename):
        """Compute the MD5 hash of a file."""
        hash_md5 = hashlib.md5()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def download_file(self, url, force_download=False):
        # Download a file from a given URL
        local_filename = os.path.join(self.DOWNLOAD_FOLDER, url.split('/')[-1])  # Define local_filename
        if not os.path.exists(self.DOWNLOAD_FOLDER):
            os.makedirs(self.DOWNLOAD_FOLDER)
            logging.info(f"Created download directory: {self.DOWNLOAD_FOLDER}")
        if os.path.exists(local_filename) and not force_download:
            # Check if the file on the server has the same MD5 hash as the local file
            response = requests.head(url)
            if 'Content-MD5' in response.headers:
                server_md5 = response.headers['Content-MD5']
                local_md5 = self.compute_md5(local_filename)
                if server_md5 == local_md5:
                    logging.info(f"File {local_filename} already exists and is identical to the server version. Skipping download.")
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

    def crawl_sublinks(self, start_url, domain):
        # Crawl sublinks of a given URL
        if not self.start_time:
            self.start_time = time.time()

        queue = deque([start_url])
        total_links = len(queue)
        while queue:
            url = queue.popleft()
            if url in self.visited_links:
                logging.info(f"URL {url} has already been visited. Skipping.")
                continue
            self.visited_links.add(url)
            html_content = self.fetch_data_from_url(url)

            self.pages_crawled += 1
            total_links = max(total_links, len(queue) + self.pages_crawled)  # Update total_links with the maximum seen so far
            logging.info(self.estimate_time_remaining(total_links))

            try:
                soup = BeautifulSoup(html_content, 'lxml')
            except Exception as e:
                logging.error(f"Error parsing content from {url}: {e}")
                continue
            for a in soup.find_all('a', href=True):
                link = urljoin(url, a['href'])
                parsed_link = urlparse(link)
                if parsed_link.netloc == domain and link not in self.visited_links:
                    if link.endswith('.xlsx') and self.robots_parser.is_allowed(self.USER_AGENT, link):
                        self.download_file(link)
                    elif not link.endswith('.xlsx'):
                        queue.append(link)
            time.sleep(self.DELAY)

    def main(self, urls):
        # Backup and Resume
        if os.path.exists("backup.json"):
            with open("backup.json", "r") as f:
                self.visited_links = set(json.load(f))
            os.remove("backup.json")

        # Main function to start the scraping process
        for url in urls:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            if domain not in self.visited_domains:
                robots_url = urljoin(url, "/robots.txt")
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
        for url in urls:
            thread = threading.Thread(target=self.crawl_sublinks, args=(url, urlparse(url).netloc))
            thread.start()
            threads.append(thread)

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        # Backup visited links after scraping
        with open("backup.json", "w") as f:
            json.dump(list(self.visited_links), f)

        logging.info(f"Visited links: {len(self.visited_links)}, Downloaded files: {len(os.listdir(self.DOWNLOAD_FOLDER))}")

if __name__ == "__main__":
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    USER_AGENT = input("Enter User-Agent (or press Enter to use default): ") or DEFAULT_USER_AGENT
    logging.info(f"Using User-Agent: {USER_AGENT}")

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

    scraper = WebScraper(user_agent=USER_AGENT)
    scraper.main(urls)
    print("\nMost recent logs from the web scraper:")
    scraper.print_logs()
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
