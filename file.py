import requests
import time
import json, os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymongo
from pymongo.errors import PyMongoError
from datetime import datetime
import pytz

MONGO_URL = os.getenv("MONGO_URL")

# MongoDB setup
try:
    client = pymongo.MongoClient(MONGO_URL)
    db = client["OT_TRADING"]
    collection = db["nine_am_data"]
except PyMongoError as e:
    print(f"❌ Failed to connect to MongoDB: {e}")
    exit(1)

TOTAL_BATCHES = 100
WORKERS_NUM = 20

def get_base_url(batch_num):
    if 1 <= batch_num <= 25:
        return "https://get-stock-live-data.vercel.app/get_stocks_data?batch_num={}"
    elif 26 <= batch_num <= 50:
        return "https://get-stock-live-data-1.vercel.app/get_stocks_data?batch_num={}"
    elif 51 <= batch_num <= 75:
        return "https://get-stock-live-data-2.vercel.app/get_stocks_data?batch_num={}"
    elif 76 <= batch_num <= 100:
        return "https://get-stock-live-data-3.vercel.app/get_stocks_data?batch_num={}"
    else:
        raise ValueError("Batch number out of range")

def fetch_batch_data(batch_num, max_retries=2):
    url = get_base_url(batch_num).format(batch_num)

    for attempt in range(max_retries + 1):
        try:
            start_time = time.perf_counter()
            response = requests.get(url, timeout=25)
            elapsed = round(time.perf_counter() - start_time, 2)

            if response.status_code == 200:
                try:
                    json_data = response.json()
                    stocks = json_data.get("stocks", [])
                    return stocks
                except json.JSONDecodeError as je:
                    print(f"❌ JSON decode error in Batch #{batch_num}: {je}")
            else:
                print(f"⚠️ Batch #{batch_num} returned status code {response.status_code} in {elapsed}s")

        except Exception as e:
            print(f"❌ Error fetching Batch #{batch_num} (Attempt {attempt+1}/{max_retries}): {e}")

        if attempt < max_retries:
            time.sleep(3)

    return None  # Indicate complete failure for this batch

def main():
    all_stocks = []
    not_fetched_batches = []

    # Step 1: Initial fetch
    print("🔄 Fetching initial batches...")
    with ThreadPoolExecutor(max_workers=WORKERS_NUM) as executor:
        futures = {executor.submit(fetch_batch_data, i): i for i in range(1, TOTAL_BATCHES + 1)}
        for future in as_completed(futures):
            batch_num = futures[future]
            result = future.result()
            if result:
                all_stocks.extend(result)
            else:
                not_fetched_batches.append(batch_num)

    # Step 2: Retry for failed batches
    if not_fetched_batches:
        print(f"🔁 Retrying {len(not_fetched_batches)} failed batches: {not_fetched_batches}")
        with ThreadPoolExecutor(max_workers=WORKERS_NUM) as executor:
            retry_futures = {executor.submit(fetch_batch_data, i): i for i in not_fetched_batches}
            for future in as_completed(retry_futures):
                batch_num = retry_futures[future]
                result = future.result()
                if result:
                    all_stocks.extend(result)
                    not_fetched_batches.remove(batch_num)

    print(f"✅ Total successful batches: {TOTAL_BATCHES - len(not_fetched_batches)}")
    print(f"❌ Batches still failed after retries: {not_fetched_batches}")

    # Step 3: Save to MongoDB
    if all_stocks:
        utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
        ist_now = utc_now.astimezone(pytz.timezone('Asia/Kolkata'))
        timestamp_str = ist_now.strftime('%Y-%m-%d %H:%M:%S')

        try:
            document = {
                "stocks": all_stocks,
                "timestamp": timestamp_str,
                "not_fetched_batches": not_fetched_batches
            }
            insert_result = collection.insert_one(document)
            new_doc_id = insert_result.inserted_id

            try:
                delete_result = collection.delete_many({"_id": {"$ne": new_doc_id}})
            except PyMongoError as e:
                print(f"❌ Error deleting old documents: {e}")

        except PyMongoError as e:
            print(f"❌ Error inserting document into MongoDB: {e}")
    else:
        print("⚠️ No stock data fetched at all.")

# Loop every 30 seconds
if __name__ == "__main__":
    while True:
        main()
        time.sleep(30)
