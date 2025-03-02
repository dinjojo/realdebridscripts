#!/usr/bin/env python3
import requests
import json
import time
import os
import logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("real_debrid_downloader.log"),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()
API_KEY = os.getenv('RD_API_KEY')
DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR', './downloads')
MAX_RETRIES = 3
MAX_CONCURRENT = 5  # Default max concurrent torrents for Real-Debrid
RATE_LIMIT_PAUSE = 1  # Seconds to wait between API calls
BULK_LIMIT = 50  # Process this many torrents at a time to avoid memory issues

# Ensure download directory exists
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

class RealDebridClient:
    BASE_URL = 'https://api.real-debrid.com/rest/1.0'

    def __init__(self, api_key):
        self.api_key = api_key
        self.auth_header = {'Authorization': f'Bearer {self.api_key}'}

    def get_torrents(self, page=1, limit=100):
        """Get torrents from Real-Debrid account with pagination

        Args:
            page: Page number to retrieve (starts at 1)
            limit: Number of items per page (max 100)
        """
        response = requests.get(
            f'{self.BASE_URL}/torrents',
            headers=self.auth_header,
            params={'page': page, 'limit': limit}
        )

        if response.status_code != 200:
            logging.error(f"Failed to get torrents page {page}: {response.text}")
            return []

        return response.json()

    def get_all_torrents(self):
        """Get all torrents with pagination handling"""
        all_torrents = []
        page = 1
        limit = 100  # Max allowed by Real-Debrid API

        while True:
            logging.info(f"Fetching torrents page {page}")
            torrents = self.get_torrents(page=page, limit=limit)

            if not torrents:
                break

            all_torrents.extend(torrents)

            if len(torrents) < limit:
                # Reached the last page
                break

            page += 1
            time.sleep(RATE_LIMIT_PAUSE)  # Respect rate limits

        logging.info(f"Retrieved a total of {len(all_torrents)} torrents")
        return all_torrents

    def get_downloads(self, page=1, limit=100):
        """Get downloads from Real-Debrid account with pagination"""
        response = requests.get(
            f'{self.BASE_URL}/downloads',
            headers=self.auth_header,
            params={'page': page, 'limit': limit}
        )

        if response.status_code != 200:
            logging.error(f"Failed to get downloads page {page}: {response.text}")
            return []

        return response.json()

    def get_all_downloads(self):
        """Get all downloads with pagination handling"""
        all_downloads = []
        page = 1
        limit = 100  # Max allowed by Real-Debrid API

        while True:
            logging.info(f"Fetching downloads page {page}")
            downloads = self.get_downloads(page=page, limit=limit)

            if not downloads:
                break

            all_downloads.extend(downloads)

            if len(downloads) < limit:
                # Reached the last page
                break

            page += 1
            time.sleep(RATE_LIMIT_PAUSE)  # Respect rate limits

        logging.info(f"Retrieved a total of {len(all_downloads)} downloads")
        return all_downloads

    def unrestrict_link(self, link):
        """Unrestrict a link for downloading"""
        # Important: Using form data instead of JSON
        data = {'link': link}
        response = requests.post(
            f'{self.BASE_URL}/unrestrict/link',
            headers=self.auth_header,
            data=data
        )

        if response.status_code != 200:
            logging.error(f"Failed to unrestrict link {link}: {response.text}")
            return None

        time.sleep(RATE_LIMIT_PAUSE)  # Respect rate limits
        return response.json()

    def get_torrent_info(self, torrent_id):
        """Get detailed information about a torrent"""
        response = requests.get(
            f'{self.BASE_URL}/torrents/info/{torrent_id}',
            headers=self.auth_header
        )

        if response.status_code != 200:
            logging.error(f"Failed to get torrent info for {torrent_id}: {response.text}")
            return None

        time.sleep(RATE_LIMIT_PAUSE)  # Respect rate limits
        return response.json()

    def select_torrent_files(self, torrent_id, file_ids='all'):
        """Select which files to download from a torrent"""
        data = {'files': file_ids}
        response = requests.post(
            f'{self.BASE_URL}/torrents/selectFiles/{torrent_id}',
            headers=self.auth_header,
            data=data
        )

        if response.status_code != 204:
            logging.error(f"Failed to select files for torrent {torrent_id}: {response.text}")
            return False

        time.sleep(RATE_LIMIT_PAUSE)  # Respect rate limits
        return True

    def get_user_limits(self):
        """Get user account limits"""
        response = requests.get(
            f'{self.BASE_URL}/user',
            headers=self.auth_header
        )

        if response.status_code != 200:
            logging.error(f"Failed to get user limits: {response.text}")
            return None

        return response.json()

def process_torrents_batch(client, torrents_batch, batch_num, total_batches):
    """Process a batch of torrents"""
#    logging.info(f"Processing batch {batch_num}/{total_batches} with {len(torrents_batch)} torrents")

    # First process completed torrents to make room for new ones
    completed = [t for t in torrents_batch if t['status'] == 'downloaded']
 #   logging.info(f"Found {len(completed)} completed torrents in batch {batch_num}")

    for index, torrent in enumerate(completed):
        torrent_id = torrent['id']
  #      logging.info(f"Processing completed torrent {index+1}/{len(completed)} in batch {batch_num}: {torrent_id}")

        torrent_info = client.get_torrent_info(torrent_id)
        if not torrent_info:
            continue

        # Get download links for completed files
        links_processed = 0
        for link in torrent_info.get('links', []):
            unrestricted = client.unrestrict_link(link)
            if unrestricted and 'download' in unrestricted:
                download_url = unrestricted['download']
                filename = unrestricted.get('filename', f"file_{int(time.time())}")
                logging.info(f"Unrestricted link for {filename}")

                # Save unrestricted links for downloading
                with open(os.path.join(DOWNLOAD_DIR, 'downloads.txt'), 'a') as f:
                    f.write(f"{filename}: {download_url}\n")

                links_processed += 1

                # Avoid hitting rate limits by processing in smaller chunks
                if links_processed % 10 == 0:
                    logging.info(f"Processed {links_processed} links for torrent {torrent_id}")
                    time.sleep(RATE_LIMIT_PAUSE * 2)  # Extra pause after processing multiple links

    # Log completion of batch
    logging.info(f"Completed processing batch {batch_num}/{total_batches}")

def process_torrents(client):
    """Process all torrents in account with batching to handle large numbers"""
    logging.info("Starting torrent processing...")

    # Get user limits to check max_torrents
    user_info = client.get_user_limits()
    if user_info and 'premium' in user_info and user_info['premium'] > 0:
        # Only premium users have torrent capabilities
        max_torrents = user_info.get('max_torrents', MAX_CONCURRENT)
        logging.info(f"Account allows maximum {max_torrents} torrents")
    else:
        logging.warning("Non-premium account detected or couldn't determine account type")
        max_torrents = MAX_CONCURRENT

    # Get all torrents with pagination
    all_torrents = client.get_all_torrents()
    logging.info(f"Found a total of {len(all_torrents)} torrents")

    # Process torrents in batches to manage memory usage
    batches = [all_torrents[i:i + BULK_LIMIT] for i in range(0, len(all_torrents), BULK_LIMIT)]
    total_batches = len(batches)

    for batch_num, torrents_batch in enumerate(batches, 1):
        process_torrents_batch(client, torrents_batch, batch_num, total_batches)
        # Small pause between batches
        if batch_num < total_batches:
            logging.info(f"Pausing between batches to respect rate limits...")
            time.sleep(RATE_LIMIT_PAUSE * 5)

def process_downloads(client):
    """Process all download links in account"""
    logging.info("Processing downloads...")

    # Get all downloads with pagination
    all_downloads = client.get_all_downloads()
    logging.info(f"Found a total of {len(all_downloads)} downloads")

    # Process in batches to respect rate limits
    for i in range(0, len(all_downloads), BULK_LIMIT):
        batch = all_downloads[i:i + BULK_LIMIT]
        logging.info(f"Processing download batch {i//BULK_LIMIT + 1}/{(len(all_downloads) + BULK_LIMIT - 1)//BULK_LIMIT}")

        for download in batch:
            link = download.get('download')  # This is the direct link
            if not link:
                continue

            filename = download.get('filename', f"file_{int(time.time())}")
            logging.info(f"Found download link for {filename}")

            # Save direct download links
            with open(os.path.join(DOWNLOAD_DIR, 'downloads.txt'), 'a') as f:
                f.write(f"{filename}: {link}\n")

            # Small pause to respect rate limits
            time.sleep(RATE_LIMIT_PAUSE)

        # Pause between batches
        if i + BULK_LIMIT < len(all_downloads):
            logging.info("Pausing between download batches...")
            time.sleep(RATE_LIMIT_PAUSE * 3)

def main():
    """Main function to process Real-Debrid files"""
    logging.info("Starting Real-Debrid Downloader")

    # Check if API key is set
    if not API_KEY:
        logging.error("API key not set. Please set RD_API_KEY in .env file")
        return

    # Run the job
    for attempt in range(MAX_RETRIES):
        try:
            client = RealDebridClient(API_KEY)
            process_torrents(client)
            process_downloads(client)
            logging.info("Job completed successfully")
            break
        except Exception as e:
            logging.error(f"Error in job (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    main()
