import requests
import time, gzip
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymongo
from pymongo.errors import PyMongoError
from datetime import datetime, timezone
import pytz
from mega import Mega

# Environment Variables
MONGO_URL = os.getenv("MONGO_URL")
M_TOKEN = os.getenv("M_TOKEN")
EXECUTION_FLAG = os.getenv("EXECUTION_FLAG")

total_count = 0
fetched_count = 0


# MongoDB Setup
try:
    client = pymongo.MongoClient(MONGO_URL)
    db = client["OT_TRADING"]
    collection = db["live_data"]
    collection_two = db["nine_am_data"]
except PyMongoError as e:
    print(f"❌ Failed to connect to MongoDB: {e}")
    exit(1)

# Constants
TOTAL_BATCHES = 100
WORKERS_NUM = 10
INDIA_TIMEZONE = pytz.timezone('Asia/Kolkata')


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

def fetch_total_stock_codes():
    global total_count
    
    url = get_base_url(0)  # batch_num < 1
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            total_stock_codes = data.get('total_stock_codes')
            if total_stock_codes is not None:
                total_count = total_stock_codes
            else:
                print("⚠️ 'total_stock_codes' not found in response")
        else:
            print(f"❌ Failed to fetch data, status code: {response.status_code}")
    except Exception as e:
        print(f"❌ Error fetching total stock codes: {e}")
        
def fetch_batch_data(batch_num, max_retries=2):
    url = get_base_url(batch_num).format(batch_num)
    global fetched_count
    for attempt in range(max_retries + 1):
        try:
            start_time = time.perf_counter()
            response = requests.get(url, timeout=25)
            elapsed_time = round(time.perf_counter() - start_time, 2)

            if response.status_code == 200:
                try:
                    data = response.json()
                    fetched_count += 1
                    return data.get("stocks", [])
                except json.JSONDecodeError as je:
                    print(f"❌ JSON decode error in Batch #{batch_num}: {je}")
            else:
                print(f"⚠️ Batch #{batch_num} returned status {response.status_code} in {elapsed_time}s")

        except Exception as e:
            print(f"❌ Error fetching Batch #{batch_num} (Attempt {attempt+1}/{max_retries}): {e}")

        if attempt < max_retries:
            print(f"🔁 Retrying Batch #{batch_num} (Attempt {attempt+2})...")
            time.sleep(3)

    print(f"❌ Failed to fetch Batch #{batch_num} after {max_retries+1} attempts.")
    return []


def fetch_all_batches():
    all_stocks = []
    with ThreadPoolExecutor(max_workers=WORKERS_NUM) as executor:
        futures = [executor.submit(fetch_batch_data, i) for i in range(1, TOTAL_BATCHES + 1)]
        for future in as_completed(futures):
            all_stocks.extend(future.result())
    return all_stocks


def insert_new_stock_data(stocks):
    if not stocks:
        print("⚠️ No stock data fetched, skipping DB update.")
        return None
    global fetched_count,total_count
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now.astimezone(INDIA_TIMEZONE)
    timestamp_str = ist_now.strftime('%Y-%m-%d %H:%M:%S')

    document = {
        "stocks": stocks,
        "timestamp": timestamp_str,
        "fetched_count":fetched_count,
        "total_count":total_count
    }

    try:
        insert_result = collection.insert_one(document)
        document["_id"] = str(insert_result.inserted_id)

        # Clean old documents
        try:
            delete_result = collection.delete_many({"_id": {"$ne": insert_result.inserted_id}})
        except PyMongoError as e:
            print(f"❌ Error deleting old documents: {e}")

        return document

    except PyMongoError as e:
        print(f"❌ Error inserting document into MongoDB: {e}")
        return None


def save_data_to_json_file(data_list):
    now = datetime.now(INDIA_TIMEZONE)
    file_name = now.strftime("%d-%m-%y_%H") + ".json.gz"  # .gz extension

    structured_data = [{
        "Data": data_list,
        "MetaData": data_list[-1]  # last element is latest
    }]

    json_data = json.dumps(structured_data, ensure_ascii=False, indent=4)
    json_bytes = json_data.encode('utf-8')  # encode to bytes

    with gzip.open(file_name, "wb") as f:
        f.write(json_bytes)

    print(f"✅ Saved and compressed data to {file_name}")

    return file_name



def upload_to_mega(file_path):
    try:
        mega = Mega()
        keys = M_TOKEN.split("_")
        m = mega.login(keys[0], keys[1])
        m.upload(file_path)
        print(f"✅ Uploaded {file_path} to Mega")
    except Exception as e:
        print(f"❌ Error uploading file to Mega: {e}")


def insert_into_nine_am_data(meta_data):
    now = datetime.now(INDIA_TIMEZONE)
    if now.hour > 15:
        try:
            collection_two.insert_one(meta_data)
            print(f"✅ Inserted meta_data into 'nine_am_data' collection at hour {now.hour}")
        except PyMongoError as e:
            print(f"❌ Error inserting into nine_am_data collection: {e}")


def should_run():
    now = datetime.now(INDIA_TIMEZONE)
    weekday = now.weekday()
    current_hour = now.hour

    if 0 <= weekday <= 4 and (9 <= current_hour < 16):
        return True
    if int(EXECUTION_FLAG) > 0:
        return True
    return False


def main():
    collected_data = []
    try:
        infinite_loop_flag = True
        fetch_total_stock_codes()

        while infinite_loop_flag:
            if should_run():
                stocks = fetch_all_batches()
                inserted_doc = insert_new_stock_data(stocks)

                if inserted_doc:
                    collected_data.append(inserted_doc)

                if int(EXECUTION_FLAG) > 0:
                    infinite_loop_flag = False
            else:
                print("⏳ Outside allowed days/hours. Stopping Execution.")
                break

            time.sleep(30)

    finally:
        if collected_data:
            file_name = save_data_to_json_file(collected_data)
            upload_to_mega(file_name)
            insert_into_nine_am_data(collected_data[-1])


if __name__ == "__main__":
    main()
