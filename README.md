# Faulty API Server — Resilient Client

Two HTTP clients that fetch 1 000 orders from a rate-limited, intentionally flaky mock API and write the results to CSV.

## Prerequisites

- Python >= 3.13
- [uv](https://docs.astral.sh/uv/) package manager

## Quick Start

```bash
# Install dependencies
uv sync

# Start the mock server (runs on http://127.0.0.1:8000)
uv run orders_server

# In another terminal — run one or both clients
uv run python client_threads.py   # -> items_threads.csv
uv run python client_async.py     # -> items_async.csv
```

## Server Behaviour

| Aspect       | Detail                                               |
|--------------|------------------------------------------------------|
| Endpoint     | `GET /item/{id}` (IDs 1–1000)                        |
| Rate limit   | 20 req/s per IP — returns **429** + `Retry-After: 1` |
| Flaky errors | ~10 % of requests return **500**                     |
| Latency      | 50–150 ms simulated delay per request                |

## Client Implementations

Both clients share the same resilience logic and produce identical CSV schemas; they only differ in the concurrency model.

### `client_threads.py` — Synchronous with Threads

| Component         | Implementation                                        |
|-------------------|-------------------------------------------------------|
| HTTP client       | `httpx.Client` (synchronous)                          |
| Concurrency       | `concurrent.futures.ThreadPoolExecutor` (10 workers)  |
| Rate limiting     | `ratelimit` — `@sleep_and_retry` + `@limits(18, 1)`  |
| Result collection | `as_completed()` writes rows in the main thread       |

### `client_async.py` — Fully Asynchronous

| Component         | Implementation                                             |
|-------------------|------------------------------------------------------------|
| HTTP client       | `httpx.AsyncClient`                                        |
| Concurrency       | `asyncio.create_task` + `Semaphore(50)` for in-flight cap  |
| Rate limiting     | `aiolimiter.AsyncLimiter(18, 1)`                           |
| Result collection | `asyncio.gather()` then sequential CSV write               |

### Retry Strategy (both clients)

| Condition               | Action                                           |
|-------------------------|--------------------------------------------------|
| **HTTP 429**            | Read `Retry-After` header, sleep, retry          |
| **HTTP 5xx / timeout**  | Sleep 1 s, retry                                 |
| **Other 4xx**           | Non-retryable — log `ERROR`, skip that ID        |
| **Max retries (5)**     | Log `ERROR`, skip that ID                        |

### Logging

- `WARNING` on every transient retry (429, 5xx, timeout).
- `ERROR` when retries are exhausted or a non-retryable status is received.
- All output goes to stdout.

## CSV Output

Both clients produce a flat CSV (no nested objects) with these columns:

```
order_id, account_id, company, status, currency, subtotal, tax, total, created_at
```

Nested fields (`contact`, `lines`) from the API response are intentionally dropped.

## Dependencies

| Package      | Purpose                              |
|--------------|--------------------------------------|
| httpx        | HTTP client (sync & async)           |
| ratelimit    | Client-side rate limiting (threads)  |
| aiolimiter   | Client-side rate limiting (async)    |
| orders-server| Mock API server package              |
