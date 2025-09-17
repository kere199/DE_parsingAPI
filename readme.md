
DE Parsing API Client

Threaded and async clients for fetching orders from a faulty API server.

Setup





Ensure Python 3.13+ is installed (python3.13 --version).



Copy orders_server-0.1.0.tar.gz to project directory.



Create virtualenv: python3.13 -m venv venv && source venv/bin/activate.



Install server: pip install orders_server-0.1.0.tar.gz.



Install client dependencies: pip install -r requirements.txt.



Run server: orders_server &.



Run threaded client: python client_threads.py or async client: python client_async.py.

How It Works

Threaded Client (client_threads.py)





Fetches orders for IDs 1–1000 using ThreadPoolExecutor (10 workers).



Rate limited to 18 req/sec via ratelimit.



Retries (max 3): 429 uses Retry-After; 5xx/timeouts sleep 1s; non-429 4xx skipped.



Logs retries (WARNING) and failures (ERROR) to stdout.



Writes to items_threads.csv with thread-safe csv.DictWriter (Pandas/Excel compatible).



Stops after 1000 successful rows or processing all IDs (1–1000).



Achieved 998 successful rows in testing, likely due to server 5xx errors.

Async Client (client_async.py)





Fetches orders for IDs 1–1000 using asyncio and httpx.AsyncClient.



Rate limited to 18 req/sec via aiolimiter.AsyncLimiter.



Concurrency capped at 50 in-flight requests with asyncio.Semaphore.



Retries (max 3): 429 uses Retry-After; 5xx/timeouts sleep 1s; non-429 4xx skipped.



Logs retries (WARNING) and failures (ERROR) to stdout.



Writes to items_async.csv asynchronously with aiofiles and csv.DictWriter.



Stops after processing all IDs (1–1000).



Achieved ~998 successful rows in testing, similar to threaded client.

Output





items_threads.csv / items_async.csv: Flat fields (order_id, account_id, company, status, currency, subtotal, tax, total, created_at).



Console logs show retry/failure details (e.g., 5xx retries).



Output: ~998 rows (some failures expected due to server errors).

Notes





Requires Python 3.13+ for server compatibility.



Threaded client uses filelock for thread-safe CSV writes.



Async client uses aiofiles for asynchronous CSV writes.



Server runs on http://127.0.0.1:8000.



~998 rows is near target; minor losses expected due to server faults.