import requests
import time
import json, os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymongo
from pymongo.errors import PyMongoError
from datetime import datetime, timezone
import pytz
from mega import Mega

MONGO_URL = os.getenv("MONGO_URL")
M_TOKEN = os.getenv("M_TOKEN")
EXECUTION_FLAG = os.getenv("EXECUTION_FLAG")
# MongoDB setup
try:
    client = pymongo.MongoClient(MONGO_URL)
    db = client["OT_TRADING"]
    collection = db["nine_am_data"]
except PyMongoError as e:
    print(f"❌ Failed to connect to MongoDB: {e}")
    exit(1)

TOTAL_BATCHES = 100
WORKERS_NUM = 10

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
        start_time = time.perf_counter()
        try:
            response = requests.get(url, timeout=25)
            end_time = time.perf_counter()
            elapsed = round(end_time - start_time, 2)

            if response.status_code == 200:
                try:
                    json_data = response.json()
                    stocks = json_data.get("stocks", [])
                    # print(f"✅ Batch #{batch_num} completed in {elapsed}s with {len(stocks)} stocks.")
                    return stocks
                except json.JSONDecodeError as je:
                    print(f"❌ JSON decode error in Batch #{batch_num}: {je}")
            else:
                print(f"⚠️ Batch #{batch_num} returned status code {response.status_code} in {elapsed}s")

        except Exception as e:
            print(f"❌ Error fetching Batch #{batch_num} (Attempt {attempt+1}/{max_retries}): {e}")

        if attempt < max_retries:
            print(f"🔁 Retrying Batch #{batch_num} (Attempt {attempt+2})...")
            time.sleep(3)

    print(f"❌ Failed to fetch Batch #{batch_num} after {max_retries+1} attempts.")
    return []

def main():
    # print(f"\n⏱️ Running main() at {datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # Step 1: Fetch all batch data in parallel
    all_stocks = []
    try:
        with ThreadPoolExecutor(max_workers=WORKERS_NUM) as executor:
            futures = [executor.submit(fetch_batch_data, i) for i in range(1, TOTAL_BATCHES + 1)]
            for future in as_completed(futures):
                all_stocks.extend(future.result())
    except Exception as e:
        print(f"❌ Error during batch data fetching: {e}")
        return

    # Step 2: Insert new data to MongoDB
    if all_stocks:
        
        # Convert UTC time to IST
        utc_now = datetime.now(timezone.utc)
        ist_now = utc_now.astimezone(pytz.timezone('Asia/Kolkata'))
        
        # Format the timestamp in the required format (YYYY-MM-DD HH:MM:SS)
        timestamp_str = ist_now.strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            document = {
                "stocks": all_stocks,
                "timestamp": timestamp_str
            }
            insert_result = collection.insert_one(document)
            new_doc_id = insert_result.inserted_id
            # print(f"✅ New stock data inserted with _id: {new_doc_id}")

            # Step 3: Delete all other old documents except the new one
            try:
                delete_result = collection.delete_many({"_id": {"$ne": new_doc_id}})
                # print(f"🧹 Deleted {delete_result.deleted_count} old document(s) from MongoDB.")
            except PyMongoError as e:
                print(f"❌ Error deleting old documents: {e}")
            if document:
                document["_id"] = str(new_doc_id)  # convert ObjectId to string
                return document
        except PyMongoError as e:
            print(f"❌ Error inserting document into MongoDB: {e}")

    else:
        print("⚠️ No stock data fetched, skipping DB update.")

    # print("✅ main() run completed.\n")


if __name__ == "__main__":
    india_timezone = pytz.timezone('Asia/Kolkata')  # IST timezone
    try:
        final_data = []
        lst = []
        infinite_loop_flag = True
        while infinite_loop_flag:
            now = datetime.now(india_timezone)  # get current IST time
            weekday = now.weekday()  # Monday = 0, Sunday = 6
            current_hour = now.hour
            
            # Check if today is Monday to Friday and time is between 9:00 and 15:59
            if 0 <= weekday <= 4 and (9 <= current_hour < 16):
                can_run = True
                
            elif int(EXECUTION_FLAG) >0:
                 
                can_run = True
                
                # If Execution flag is true then its for testing purpose of one time only
                infinite_loop_flag = False
                
            else:
                can_run = False
            
            if can_run:
                result = main()
                if result: 
                    final_data.append(result)
            else:
                print("⏳ Not (Outside allowed days/hours) Stopping Execution")
                break
            
            time.sleep(30)

    finally:
        if len(final_data)>0:
            meta_data = final_data[-1]
            lst.append({
                "Data":final_data,
                "MetaData":meta_data
            })
            # Save final_data to a JSON file with today's date (dd-mm-yy.json)
            now = datetime.now(india_timezone)
            file_name = now.strftime("%d-%m-%y") + ".json"
            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(lst, f, ensure_ascii=False, indent=4)
            print(f"✅ Saved collected data to {file_name}")

            mega = Mega()
            keys = M_TOKEN.split("_")
            m = mega.login(keys[0],keys[1])
            try:
                m.upload(file_name)
            except Exception as e:
                print("Error failed to upload file : ",e)
