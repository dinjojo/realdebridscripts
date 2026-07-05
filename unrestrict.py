"""
Real-Debrid link extractor — optimised for the 250 req/min API limit.

Key changes vs. previous version:
  1. RateLimiter.penalize() — any 429 globally stalls ALL threads, not just
     the one that hit the limit. Prevents the "all wake up at once" burst.
  2. acquire() is called ONCE per logical request (outside the retry loop).
     Retries reuse the same token — they are the same request, not new ones.
  3. Single shared ThreadPoolExecutor across all torrents — true global cap
     on concurrent connections (no more per-torrent pool bursts).
  4. Pagination at limit=5000 + X-Total-Count header awareness — fetches
     the entire torrent/download list in one or two calls instead of dozens.
  5. Retry on 429 no longer loops inside _request; instead it calls
     penalize() and re-enters the queue so all workers slow down together.
"""

import requests
import requests.exceptions
import time
import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("real_debrid_downloader.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()
API_KEY      = os.getenv('RD_API_KEY')
DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR', './downloads')
MAX_RETRIES  = 3
BULK_LIMIT   = 50

# RD hard limit: 250 req/min.  We target 200/min (20% headroom) so that
# normal variance never touches the ceiling.
RD_REQUESTS_PER_MIN = 200

# True global cap — one shared pool, never more than this many simultaneous
# TCP connections open to RD at the same time.
RD_MAX_CONCURRENT = 8

# Max page size supported by the RD API (torrents, downloads).
RD_PAGE_LIMIT = 5000

RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Token-bucket rate limiter with global penalty support
# ---------------------------------------------------------------------------
class RateLimiter:
    """
    Token-bucket limiter with a penalize() method.

    When any thread receives a 429, it calls penalize(seconds).  This:
      - Sets a "blocked until" timestamp visible to every thread.
      - Drains the token bucket so no burst occurs when the penalty expires.

    All threads check the penalty in acquire() before consuming a token,
    so the cooldown is effectively global — no thread races ahead while
    others are still sleeping.
    """

    def __init__(self, requests_per_minute: int):
        self._rate          = requests_per_minute / 60.0
        self._capacity      = float(requests_per_minute)
        self._tokens        = float(requests_per_minute)
        self._last_tick     = time.monotonic()
        self._penalty_until = 0.0   # monotonic timestamp; 0 = no penalty
        self._lock          = threading.Lock()

    def penalize(self, seconds: float):
        """
        Called on HTTP 429.  Stalls ALL threads for `seconds` and drains
        the token bucket so the burst doesn't immediately resume.
        """
        with self._lock:
            resume = time.monotonic() + seconds
            if resume > self._penalty_until:
                self._penalty_until = resume
            self._tokens = 0.0  # drain: forces refill delay after penalty
        logging.warning(f"[RateLimiter] Global penalty set — all threads paused for {seconds:.1f}s")

    def acquire(self):
        """Block until a request token is available."""
        # --- 1. Honour any active global penalty first ---
        while True:
            with self._lock:
                remaining = self._penalty_until - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(remaining, 1.0))  # wake up in ≤1s slices to recheck

        # --- 2. Normal token-bucket logic ---
        while True:
            with self._lock:
                now     = time.monotonic()
                elapsed = now - self._last_tick
                self._tokens    = min(self._capacity, self._tokens + elapsed * self._rate)
                self._last_tick = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                wait = (1.0 - self._tokens) / self._rate

            time.sleep(wait)


# ---------------------------------------------------------------------------
# Real-Debrid API client
# ---------------------------------------------------------------------------
class RealDebridClient:
    BASE_URL = 'https://api.real-debrid.com/rest/1.0'

    def __init__(self, api_key: str, limiter: RateLimiter):
        self._limiter = limiter
        self.session  = requests.Session()
        self.session.headers.update({'Authorization': f'Bearer {api_key}'})

        adapter = HTTPAdapter(
            pool_connections=RD_MAX_CONCURRENT,
            pool_maxsize=RD_MAX_CONCURRENT,
            max_retries=0,   # we handle retries ourselves
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://',  adapter)

    # -----------------------------------------------------------------------
    # Core HTTP helper
    # -----------------------------------------------------------------------
    def _request(self, method: str, path: str, **kwargs):
        """
        Single logical request with connection-error retries.

        IMPORTANT: acquire() is called exactly ONCE per logical request,
        BEFORE the retry loop.  Retries are not new requests — they must not
        consume additional rate-limit tokens.

        429 handling:
          - Calls limiter.penalize() → all threads pause globally.
          - Then re-acquires a token (which will block until penalty clears)
            and retries the call.
        """
        url = f'{self.BASE_URL}/{path}'
        kwargs.setdefault('timeout', 30)

        # Acquire one token for this logical request.
        self._limiter.acquire()

        for attempt in range(5):
            try:
                response = getattr(self.session, method)(url, **kwargs)

                if response.status_code == 429:
                    # Exponential penalty: 10s, 20s, 40s, 80s, 160s
                    # (larger base than before; 429s are expensive under RD rules)
                    penalty = 10 * (2 ** attempt)
                    logging.warning(
                        f"[429] {path} — applying {penalty}s global penalty (attempt {attempt + 1}/5)"
                    )
                    self._limiter.penalize(penalty)
                    # Re-acquire: this blocks until the penalty clears, then
                    # waits for a fresh token — no extra burst when we resume.
                    self._limiter.acquire()
                    continue

                return response

            except RETRYABLE_EXCEPTIONS as e:
                wait = 3 * (2 ** attempt)   # 3 → 6 → 12 → 24 → 48s
                logging.warning(
                    f"[ConnError] {e.__class__.__name__} on {path} — retry in {wait}s "
                    f"(attempt {attempt + 1}/5)"
                )
                time.sleep(wait)

        logging.error(f"Gave up on {path} after 5 attempts")
        return None

    # -----------------------------------------------------------------------
    # Pagination — uses X-Total-Count header + limit=5000 to minimise calls
    # -----------------------------------------------------------------------
    def _get_all(self, endpoint: str):
        """
        Fetches all items from a paginated endpoint.

        Uses page= (not offset=) — the RD API is more reliable with page-based
        pagination.  limit=5000 means most accounts finish in a single call.
        X-Total-Count lets us stop early without an extra empty-page round-trip.
        """
        results = []
        page    = 1

        while True:
            logging.info(f"Fetching {endpoint} (page={page}, limit={RD_PAGE_LIMIT})…")
            response = self._request(
                'get', endpoint,
                params={'page': page, 'limit': RD_PAGE_LIMIT}
            )

            if response is None or response.status_code != 200:
                if response:
                    logging.error(
                        f"Failed to fetch {endpoint} page {page} "
                        f"[HTTP {response.status_code}]: {response.text}"
                    )
                break

            page_data = response.json()
            results.extend(page_data)

            total = int(response.headers.get('X-Total-Count', len(results)))
            logging.info(
                f"  Page {page}: got {len(page_data)} items "
                f"(total: {total}, fetched: {len(results)})"
            )

            if len(results) >= total or len(page_data) < RD_PAGE_LIMIT:
                break   # got everything

            page += 1

        logging.info(f"Fetched {len(results)} items from {endpoint}")
        return results

    def get_all_torrents(self):
        return self._get_all('torrents')

    def get_all_downloads(self):
        return self._get_all('downloads')

    # -----------------------------------------------------------------------
    # Single-item calls
    # -----------------------------------------------------------------------
    def get_torrent_info(self, torrent_id: str):
        response = self._request('get', f'torrents/info/{torrent_id}')
        if response is None or response.status_code != 200:
            if response:
                logging.error(f"Failed to get torrent info {torrent_id} [HTTP {response.status_code}]: {response.text}")
            return None
        return response.json()

    def unrestrict_link(self, link: str):
        response = self._request('post', 'unrestrict/link', data={'link': link})
        if response is None or response.status_code != 200:
            if response:
                logging.error(f"Failed to unrestrict {link} [HTTP {response.status_code}]: {response.text}")
            return None
        return response.json()

    def get_user_info(self):
        response = self._request('get', 'user')
        if response is None or response.status_code != 200:
            if response:
                logging.error(f"Failed to get user info: {response.text}")
            return None
        return response.json()


# ---------------------------------------------------------------------------
# Work items for the shared thread pool
# ---------------------------------------------------------------------------
def _unrestrict_one(client: RealDebridClient, link: str, torrent_id: str, idx: int):
    """Unrestrict a single link; returns (filename, download_url) or None."""
    result = client.unrestrict_link(link)
    if result and 'download' in result:
        filename = result.get('filename') or f"file_{torrent_id}_{idx}"
        return filename, result['download']
    return None


# ---------------------------------------------------------------------------
# Torrent processing — uses a single shared executor
# ---------------------------------------------------------------------------
def process_torrents(client: RealDebridClient):
    logging.info("=== Torrents ===")

    user_info = client.get_user_info()
    if user_info and user_info.get('premium', 0) > 0:
        logging.info(
            f"Account: Premium — expires in {user_info['premium'] // 86400}d "
            f"(max torrents: {user_info.get('max_torrents', '?')})"
        )
    else:
        logging.warning("Account: Non-premium or unknown")

    all_torrents = list(reversed(client.get_all_torrents()))   # oldest first
    completed    = [t for t in all_torrents if t['status'] == 'downloaded']
    logging.info(f"Found {len(all_torrents)} torrents, {len(completed)} completed")

    # ------------------------------------------------------------------
    # One shared executor for the entire run.
    # This is the critical fix: previously a new pool was created per torrent,
    # meaning up to (RD_MAX_CONCURRENT × batched_torrents) connections could
    # be open simultaneously.  A single pool caps the total.
    # ------------------------------------------------------------------
    out_path = os.path.join(DOWNLOAD_DIR, 'downloads.txt')
    with open(out_path, 'a') as out_file, \
         ThreadPoolExecutor(max_workers=RD_MAX_CONCURRENT) as pool:

        for index, torrent in enumerate(completed):
            torrent_id = torrent['id']
            name       = torrent.get('filename', torrent_id)

            torrent_info = client.get_torrent_info(torrent_id)
            if not torrent_info:
                logging.warning(f"  Skipping {name} — no torrent info")
                continue

            links = torrent_info.get('links', [])
            if not links:
                logging.warning(f"  Skipping {name} — no links")
                continue

            # Submit all links for this torrent to the shared pool
            futures = {
                pool.submit(_unrestrict_one, client, link, torrent_id, i): i
                for i, link in enumerate(links)
            }

            links_saved = 0
            for future in as_completed(futures):
                result = future.result()
                if result:
                    filename, download_url = result
                    out_file.write(f"{filename}: {download_url}\n")
                    links_saved += 1

            out_file.flush()
            logging.info(
                f"  Torrent {index + 1}/{len(completed)}: "
                f"{links_saved}/{len(links)} links saved for {name[:60]}"
            )

    logging.info("Torrents done.")


# ---------------------------------------------------------------------------
# Downloads processing (already unrestricted — no unrestrict calls needed)
# ---------------------------------------------------------------------------
def process_downloads(client: RealDebridClient):
    logging.info("=== Downloads ===")
    all_downloads = client.get_all_downloads()
    logging.info(f"Found {len(all_downloads)} downloads")

    out_path = os.path.join(DOWNLOAD_DIR, 'downloads.txt')
    saved    = 0
    with open(out_path, 'a') as out_file:
        for i, download in enumerate(all_downloads):
            link = download.get('download')
            if not link:
                continue
            filename = download.get('filename') or f"file_{i}"
            out_file.write(f"{filename}: {link}\n")
            saved += 1

    logging.info(f"Saved {saved} download links")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    logging.info("Starting Real-Debrid Downloader")
    logging.info(
        f"Rate limit: {RD_REQUESTS_PER_MIN} req/min target, "
        f"{RD_MAX_CONCURRENT} concurrent workers"
    )

    if not API_KEY:
        logging.error("API key not set — add RD_API_KEY to your .env file")
        return

    limiter = RateLimiter(RD_REQUESTS_PER_MIN)

    for attempt in range(MAX_RETRIES):
        try:
            client = RealDebridClient(API_KEY, limiter)
            process_torrents(client)
            process_downloads(client)
            logging.info("Done. All links saved to downloads.txt")
            break
        except Exception as e:
            logging.error(f"Unhandled error (attempt {attempt + 1}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt < MAX_RETRIES - 1:
                logging.info("Retrying in 60 seconds…")
                time.sleep(60)


if __name__ == "__main__":
    main()
