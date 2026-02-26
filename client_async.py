"""
Asynchronous client for the Orders mock API.

Fetches order IDs 1–1000 concurrently using asyncio + httpx.AsyncClient,
handles rate-limiting (429) and transient server errors (5xx) with
retries, and writes the flat results to items_async.csv.
"""

import asyncio
import csv
import logging
import time

import httpx
from aiolimiter import AsyncLimiter

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "http://127.0.0.1:8000"
TOTAL_IDS = 1000
MAX_CONCURRENCY = 50
MAX_RETRIES = 5
REQUEST_TIMEOUT = 10.0
CALLS_PER_SECOND = 18
CSV_FILE = "items_async.csv"

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
# Rate limiter & concurrency semaphore
# ---------------------------------------------------------------------------
limiter = AsyncLimiter(CALLS_PER_SECOND, 1)
semaphore = asyncio.Semaphore(MAX_CONCURRENCY)


# ---------------------------------------------------------------------------
# Rate-limited HTTP call
# ---------------------------------------------------------------------------

async def _rate_limited_get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """Perform a GET request respecting the client-side rate limit."""
    async with limiter:
        return await client.get(url, timeout=REQUEST_TIMEOUT)


# ---------------------------------------------------------------------------
# Worker: fetch a single order with retry logic
# ---------------------------------------------------------------------------

async def fetch_and_clean_order(
    item_id: int,
    client: httpx.AsyncClient,
) -> dict | None:
    """Fetch one order, retry on transient errors, return flat dict or None."""

    url = f"{BASE_URL}/item/{item_id}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                response = await _rate_limited_get(client, url)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            logger.warning(
                "ID %d – attempt %d/%d – transport error: %s. Retrying in 1 s …",
                item_id, attempt, MAX_RETRIES, exc,
            )
            await asyncio.sleep(1)
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
            await asyncio.sleep(wait)
            continue

        if 500 <= response.status_code < 600:
            logger.warning(
                "ID %d – attempt %d/%d – server error %d. Retrying in 1 s …",
                item_id, attempt, MAX_RETRIES, response.status_code,
            )
            await asyncio.sleep(1)
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

async def main() -> None:
    start = time.perf_counter()
    rows_written = 0

    async with httpx.AsyncClient() as client:
        tasks = [
            asyncio.create_task(fetch_and_clean_order(item_id, client))
            for item_id in range(1, TOTAL_IDS + 1)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
        writer.writeheader()

        for item_id, result in enumerate(results, start=1):
            if isinstance(result, Exception):
                logger.error("ID %d – unexpected exception", item_id, exc_info=result)
                continue
            if result is not None:
                writer.writerow(result)
                rows_written += 1

    elapsed = time.perf_counter() - start
    logger.info(
        "Done – %d/%d rows written to %s in %.1f s",
        rows_written, TOTAL_IDS, CSV_FILE, elapsed,
    )


if __name__ == "__main__":
    asyncio.run(main())
