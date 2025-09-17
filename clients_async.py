import asyncio
import httpx
import logging
import csv
import aiofiles
from aiolimiter import AsyncLimiter
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CALLS = 18
PERIOD = 1
MAX_RETRIES = 3
FIELDNAMES = ["order_id", "account_id", "company", "status", "currency", "subtotal", "tax", "total", "created_at"]
BASE_URL = "http://127.0.0.1:8000/item/"
OUTPUT_FILE = "items_async.csv"

async def fetch_item(client, item_id, limiter, semaphore):
    """Fetch an item by ID with rate limiting and retries."""
    for attempt in range(MAX_RETRIES):
        async with semaphore, limiter:
            try:
                response = await client.get(f"{BASE_URL}{item_id}", timeout=2.0)
                if response.status_code == 200:
                    data = response.json()
                    return {
                        "order_id": data["order_id"],
                        "account_id": data["account_id"],
                        "company": data["company"],
                        "status": data["status"],
                        "currency": data["currency"],
                        "subtotal": data["subtotal"],
                        "tax": data["tax"],
                        "total": data["total"],
                        "created_at": data["created_at"]
                    }
                elif response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 1))
                    logging.warning(f"429 for ID {item_id}, attempt {attempt + 1}/{MAX_RETRIES}, sleeping {retry_after}s")
                    await asyncio.sleep(retry_after)
                elif response.status_code >= 500:
                    logging.warning(f"5xx for ID {item_id}, attempt {attempt + 1}/{MAX_RETRIES}, sleeping 1s")
                    await asyncio.sleep(1)
                else:
                    logging.error(f"Failed ID {item_id}: Status {response.status_code}")
                    return None
            except httpx.TimeoutException:
                logging.warning(f"Timeout for ID {item_id}, attempt {attempt + 1}/{MAX_RETRIES}, sleeping 1s")
                await asyncio.sleep(1)
            except httpx.HTTPError as e:
                logging.error(f"HTTP error for ID {item_id}: {e}")
                return None
    logging.error(f"Exhausted retries for ID {item_id}")
    return None

async def write_to_csv(data):
    """Write data to CSV asynchronously."""
    if not data:
        return
    async with aiofiles.open(OUTPUT_FILE, mode="a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        await f.write("\n")
        await writer.writerow(data)

async def main():
    """Main async function to fetch orders and write to CSV."""
    async with aiofiles.open(OUTPUT_FILE, mode="w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        await writer.writeheader()

    limiter = AsyncLimiter(CALLS, PERIOD)
    semaphore = asyncio.Semaphore(50)
    successful = 0

    async with httpx.AsyncClient() as client:
        tasks = [fetch_item(client, i, limiter, semaphore) for i in range(1, 1001)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict):
                await write_to_csv(result)
                successful += 1

    logging.info(f"Completed. Total successful items written to {OUTPUT_FILE}: {successful}")

if __name__ == "__main__":
    asyncio.run(main())