"""
Threaded synchronous client for the Orders mock API.

Fetches order IDs 1 to 1000 concurrently using a ThreadPoolExecutor,
handles rate-limiting (429) and transient server errors (5xx) with
retries, and writes the flat results to items_threads.csv.
"""

import csv
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from ratelimit import limits, sleep_and_retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "http://127.0.0.1:8000"
TOTAL_IDS = 1000
MAX_WORKERS = 10
MAX_RETRIES = 5
REQUEST_TIMEOUT = 10.0
CALLS_PER_SECOND = 18
CSV_FILE = "items_threads.csv"

CSV_FIELDS = [
    "order_id",
    "account_id",
    "company",
    "status",
    "currency",
    "subtotal",
    "tax",
    "total",
    "created_at",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate-limited HTTP call
# ---------------------------------------------------------------------------

@sleep_and_retry
@limits(calls=CALLS_PER_SECOND, period=1)
def _rate_limited_get(client: httpx.Client, url: str) -> httpx.Response:
    """Perform a GET request respecting the client-side rate limit."""
    return client.get(url, timeout=REQUEST_TIMEOUT)


# ---------------------------------------------------------------------------
# Worker: fetch a single order with retry logic
# ---------------------------------------------------------------------------

def fetch_and_clean_order(
    item_id: int,
    client: httpx.Client,
) -> dict | None:
    """Fetch one order, retry on transient errors, return flat dict or None."""

    url = f"{BASE_URL}/item/{item_id}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = _rate_limited_get(client, url)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            logger.warning(
                "ID %d – attempt %d/%d – transport error: %s. Retrying in 1 s …",
                item_id, attempt, MAX_RETRIES, exc,
            )
            time.sleep(1)
            continue

        if response.status_code == 200:
            data = response.json()
            return {field: data[field] for field in CSV_FIELDS}

        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 1))
            logger.warning(
                "ID %d – attempt %d/%d – 429 rate-limited. Sleeping %d s …",
                item_id, attempt, MAX_RETRIES, wait,
            )
            time.sleep(wait)
            continue

        if 500 <= response.status_code < 600:
            logger.warning(
                "ID %d – attempt %d/%d – server error %d. Retrying in 1 s …",
                item_id, attempt, MAX_RETRIES, response.status_code,
            )
            time.sleep(1)
            continue

        # Any other 4xx → non-retryable
        logger.error(
            "ID %d – non-retryable client error %d: %s",
            item_id, response.status_code, response.text,
        )
        return None

    logger.error("ID %d – all %d retries exhausted.", item_id, MAX_RETRIES)
    return None


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    start = time.perf_counter()
    rows_written = 0

    with (
        httpx.Client() as client,
        open(CSV_FILE, "w", newline="", encoding="utf-8") as csvfile,
    ):
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
        writer.writeheader()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(fetch_and_clean_order, item_id, client): item_id
                for item_id in range(1, TOTAL_IDS + 1)
            }

            for future in as_completed(futures):
                item_id = futures[future]
                try:
                    row = future.result()
                except Exception:
                    logger.exception("ID %d – unexpected exception", item_id)
                    continue

                if row is not None:
                    writer.writerow(row)
                    rows_written += 1

    elapsed = time.perf_counter() - start
    logger.info(
        "Done – %d/%d rows written to %s in %.1f s",
        rows_written, TOTAL_IDS, CSV_FILE, elapsed,
    )


if __name__ == "__main__":
    main()
