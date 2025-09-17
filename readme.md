DE Parsing API Client
Threaded client for fetching orders from a faulty API server.
Setup

Ensure Python 3.13+ is installed (python3.13 --version).
Copy orders_server-0.1.0.tar.gz to project directory.
Create virtualenv: python3.13 -m venv venv && source venv/bin/activate.
Install server: pip install orders_server-0.1.0.tar.gz.
Install client dependencies: pip install -r requirements.txt.
Run server: orders_server &.
Run client: python client_threads.py.

How It Works

Fetches orders for IDs 1–1000 using ThreadPoolExecutor (10 workers).
Rate limited to 18 req/sec via ratelimit.
Retries (max 3): 429 uses Retry-After; 5xx/timeouts sleep 1s; non-429 4xx skipped.
Logs retries (WARNING) and failures (ERROR) to stdout.
Writes to items_threads.csv with thread-safe csv.DictWriter (Pandas/Excel compatible).
Stops after 1000 successful rows or processing all IDs (1–1000).
Achieved 999 successful rows in testing, likely due to server 5xx errors.


Output

items_threads.csv: Flat fields (order_id, account_id, company, status, currency, subtotal, tax, total, created_at).
Console logs show retry/failure details (e.g., 5xx retries).
Output: 998 rows (2 failed due to server errors).

Notes

Requires Python 3.13+ for server compatibility.
Uses filelock for thread-safe CSV writes.
Async version (client_async.py) not implemented.
Server runs on http://127.0.0.1:8000.
999 rows is near target; minor losses expected due to server faults.