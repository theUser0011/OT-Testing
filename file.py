import requests
import time
import json,os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymongo
from pymongo.errors import PyMongoError

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
WORKERS_NUM = 30

def get_base_url(batch_num):
    if 1 <= batch_num <= 30:
        return "https://get-stock-live-data.vercel.app/get_stocks_data?batch_num={}"
    elif 31 <= batch_num <= 60:
        return "https://get-stock-live-data-1.vercel.app/get_stocks_data?batch_num={}"
    elif 61 <= batch_num <= 100:
        return "https://get-stock-live-data-2.vercel.app/get_stocks_data?batch_num={}"
    else:
        raise ValueError("Batch number out of range")

def fetch_batch_data(batch_num):
    url = get_base_url(batch_num).format(batch_num)
    start_time = time.perf_counter()

    try:
        response = requests.get(url, timeout=30)
        end_time = time.perf_counter()
        elapsed = round(end_time - start_time, 2)

        if response.status_code == 200:
            try:
                json_data = response.json()
                stocks = json_data.get("stocks", [])
                print(f"✅ Batch #{batch_num} completed in {elapsed}s with {len(stocks)} stocks.")
                return stocks
            except json.JSONDecodeError as je:
                print(f"❌ JSON decode error in Batch #{batch_num}: {je}")
                return []
        else:
            print(f"⚠️ Batch #{batch_num} returned status code {response.status_code} in {elapsed}s")
            return []

    except requests.RequestException as e:
        print(f"❌ Network error fetching Batch #{batch_num}: {e}")
        return []

# Step 1: Fetch all batch data in parallel
all_stocks = []
try:
    with ThreadPoolExecutor(max_workers=WORKERS_NUM) as executor:
        futures = [executor.submit(fetch_batch_data, i) for i in range(1, TOTAL_BATCHES + 1)]
        for future in as_completed(futures):
            all_stocks.extend(future.result())
except Exception as e:
    print(f"❌ Error during batch data fetching: {e}")
    exit(1)

# Step 2: Insert new data to MongoDB
if all_stocks:
    try:
        document = {"stocks": all_stocks}
        insert_result = collection.insert_one(document)
        new_doc_id = insert_result.inserted_id
        print(f"✅ New stock data inserted with _id: {new_doc_id}")

        # Step 3: Delete all other old documents except the new one
        try:
            delete_result = collection.delete_many({"_id": {"$ne": new_doc_id}})
            print(f"🧹 Deleted {delete_result.deleted_count} old document(s) from MongoDB.")
        except PyMongoError as e:
            print(f"❌ Error deleting old documents: {e}")

    except PyMongoError as e:
        print(f"❌ Error inserting document into MongoDB: {e}")

print("✅ Process completed. Data saved to MongoDB.")
