import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any
import httpx
from ratelimit import limits, sleep_and_retry
from filelock import FileLock

# Configure logging to stdout
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Rate limit: 18 calls per second
CALLS = 18
PERIOD = 1

# Retry config
MAX_RETRIES = 3

# Output CSV file
CSV_FILE = 'items_threads.csv'
LOCK_FILE = 'items_threads.csv.lock'
FIELDNAMES = ['order_id', 'account_id', 'company', 'status', 'currency', 'subtotal', 'tax', 'total', 'created_at']

def write_to_csv(row: Dict[str, Any]):
    with FileLock(LOCK_FILE):
        # Check if file exists to write header only once
        try:
            with open(CSV_FILE, 'x', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writeheader()
                writer.writerow(row)
        except FileExistsError:
            with open(CSV_FILE, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writerow(row)

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def fetch_item(client: httpx.Client, item_id: int, retries: int = 0) -> Dict[str, Any] | None:
    url = f"http://127.0.0.1:8000/item/{item_id}"
    try:
        response = client.get(url, timeout=5.0)
        if response.status_code == 200:
            data = response.json()
            extracted = {
                'order_id': data.get('order_id'),
                'account_id': data.get('account_id'),
                'company': data.get('company'),
                'status': data.get('status'),
                'currency': data.get('currency'),
                'subtotal': data.get('subtotal'),
                'tax': data.get('tax'),
                'total': data.get('total'),
                'created_at': data.get('created_at')
            }
            return extracted
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 1))
            logger.warning(f"Rate limit hit for ID {item_id}. Sleeping {retry_after}s (attempt {retries + 1}/{MAX_RETRIES}).")
            if retries < MAX_RETRIES - 1:
                import time
                time.sleep(retry_after)
                return fetch_item(client, item_id, retries + 1)
            else:
                logger.error(f"Max retries exceeded for ID {item_id} after 429.")
                return None
        elif 500 <= response.status_code < 600:
            logger.warning(f"5xx error for ID {item_id} ({response.status_code}). Retrying in 1s (attempt {retries + 1}/{MAX_RETRIES}).")
            if retries < MAX_RETRIES - 1:
                import time
                time.sleep(1)
                return fetch_item(client, item_id, retries + 1)
            else:
                logger.error(f"Max retries exceeded for ID {item_id} after 5xx.")
                return None
        elif 400 <= response.status_code < 500 and response.status_code != 429:
            logger.error(f"Non-retryable 4xx error for ID {item_id} ({response.status_code}). Skipping.")
            return None
        else:
            logger.error(f"Unexpected status {response.status_code} for ID {item_id}.")
            return None
    except httpx.TimeoutException:
        logger.warning(f"Timeout for ID {item_id}. Retrying in 1s (attempt {retries + 1}/{MAX_RETRIES}).")
        if retries < MAX_RETRIES - 1:
            import time
            time.sleep(1)
            return fetch_item(client, item_id, retries + 1)
        else:
            logger.error(f"Max retries exceeded for ID {item_id} after timeout.")
            return None
    except Exception as e:
        logger.warning(f"Transport error for ID {item_id}: {e}. Retrying in 1s (attempt {retries + 1}/{MAX_RETRIES}).")
        if retries < MAX_RETRIES - 1:
            import time
            time.sleep(1)
            return fetch_item(client, item_id, retries + 1)
        else:
            logger.error(f"Max retries exceeded for ID {item_id} after transport error: {e}.")
            return None
def main():
    successful_count = 0
    with httpx.Client() as client, ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_item, client, item_id): item_id for item_id in range(1, 1001)}
        for future in as_completed(futures):
            item_id = futures[future]
            try:
                result = future.result()
                if result:
                    write_to_csv(result)
                    successful_count += 1
                    if successful_count >= 1000:
                        print(f"Reached 1000 successful items. Stopping.")
                        break
            except Exception as e:
                logger.error(f"Unexpected error processing ID {item_id}: {e}")
    print(f"Completed. Total successful items written to {CSV_FILE}: {successful_count}")

if __name__ == "__main__":
    main()