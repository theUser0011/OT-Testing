import json
import time
from nseconnect import Nse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pytz, os
from pymongo import MongoClient
from mega import Mega

# Initialize NSE instance
nse = Nse()

M_TOKEN = os.getenv("M_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")
# Fetch stock symbols
stock_codes = nse.get_stock_codes()
# stock_codes = stock_codes[:10]
client = MongoClient(MONGO_URL)

stock_symbols = [symbol for symbol in stock_codes if symbol != "SYMBOL"]

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_WORKERS = 10  # adjust based on system/network limits

# Get current time in IST
ist = pytz.timezone('Asia/Kolkata')
current_time_ist = datetime.now(ist)

# Format date as day-month-year : hour:minute (e.g., 21-04-2025 : 23:35)
formatted_date = current_time_ist.strftime("%d-%m-%Y : %H:%M")
file_name = f"{formatted_date.strip()}.json"

def save_link_to_mongodb(link,file_name):
    try:
        db = client["OT_TRADING"]
        collection = db["file_links"]
        
        # Check if document with the same date already exists
        existing_document = collection.find_one({"date": formatted_date})
        
        if existing_document:
            print("❌ Data already exists for today's date. Skipping insert.")
            return None
        else:
            # If no document with today's date exists, insert new data
            document = {
                "date": formatted_date,
                "file_name":file_name,
                "link": link
            }
            collection.insert_one(document)
            print("✅ link Data saved to MongoDB successfully.")
            return None
    except Exception as e:
        print("❌ Failed to save data to MongoDB:", e)


def save_to_mongodb(data):
    try:
        db = client["OT_TRADING"]
        collection = db["nine_am_data"]

        # Get current time in IST
        ist = pytz.timezone('Asia/Kolkata')
        current_time_ist = datetime.now(ist)

        # Format date as day-month-year : hour:minute (e.g., 21-04-2025 : 23:35)
        formatted_date = current_time_ist.strftime("%d-%m-%Y : %H:%M")

        # Check if document with the same date already exists
        existing_document = collection.find_one({"date": formatted_date})
        
        if existing_document:
            print("❌ Data already exists for today's date. Skipping insert.")
            return None
        else:
            # If no document with today's date exists, insert new data
            document = {
                "date": formatted_date,
                "data": data
            }
            collection.insert_one(document)
            print("✅ New Data saved to MongoDB successfully.")
            return None
    except Exception as e:
        print("❌ Failed to save data to MongoDB:", e)

def mround(value, round_to):
    return round_to * round(value / round_to)

def calculate_and_save(open_price, yesterday_high, yesterday_low):
    open_price = float(open_price)
    yesterday_high = float(yesterday_high)
    yesterday_low = float(yesterday_low)

    capital = 100000  # D5
    risk_per_trade = capital * 0.01  # 1% of capital

    range_value = yesterday_high - yesterday_low
    buy_entry = mround((open_price + (range_value * 0.55)), 0.05)
    sell_entry = mround((open_price - (range_value * 0.55)), 0.05)

    buy_stoploss = mround(buy_entry - (buy_entry * 0.0135), 0.05)
    sell_stoploss = mround(sell_entry + (sell_entry * 0.0135), 0.05)

    risk_buy = buy_entry - buy_stoploss
    risk_sell = sell_stoploss - sell_entry

    shares_num = mround(risk_per_trade / risk_buy, 1) if risk_buy != 0 else 0

    output = {
        "Buy_Entry": round(buy_entry, 2),
        "Sell_Entry": round(sell_entry, 2),
        "Buy_Stoploss": round(buy_stoploss, 2),
        "Sell_Stoploss": round(sell_stoploss, 2),
        "Shares_count": int(shares_num),
        "Signal": None,
        "Current_price": None,
        "Signal_Flag": None
    }

    return output

def fetch_stock_data(symbol):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Get the stock quote
            quote = nse.get_quote(symbol)

            # Fetch values, and ensure they are not None by using a default value if needed
            current_price = quote.get("lastPrice", 0.0)
            price_change = quote.get("change", 0.0)
            percentage_change = quote.get("pChange", 0.0)
            previous_close_price = quote.get("previousClose", 0.0)
            opening_price = quote.get("open", 0.0)
            vwap = quote.get("vwap", 0.0)
            daily_low = quote.get("dayLow", 0.0)
            daily_high = quote.get("dayHigh", 0.0)
            year_low = quote.get("52WeekLow", 0.0)
            year_high = quote.get("52WeekHigh", 0.0)
            lower_circuit_limit = quote.get("lowerCircuit", 0.0)
            upper_circuit_limit = quote.get("upperCircuit", 0.0)

            # Structure the stock data with the 'data' key
            stock_info = {
                "symbol": symbol,  # Stock symbol (code)
                "date": formatted_date,  # Current date and time in IST
                "data": {  # Nested data object
                    "currentPrice": current_price,
                    "priceChange": price_change,
                    "percentageChange": percentage_change,
                    "previousClosePrice": previous_close_price,
                    "openingPrice": opening_price,
                    "vwap": vwap,
                    "dailyLow": daily_low,
                    "dailyHigh": daily_high,
                    "yearLow": year_low,
                    "yearHigh": year_high,
                    "lowerCircuitLimit": lower_circuit_limit,
                    "upperCircuitLimit": upper_circuit_limit
                }
            }

            # Call the calculate_and_save function with open price, yesterday's high, and low
            stock_data = stock_info["data"]
            stock_info["calculated_data"] = calculate_and_save(
                stock_data["openingPrice"],
                stock_data["dailyHigh"],
                stock_data["dailyLow"]
            )
            return stock_info
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for {symbol}: {e}")
            if attempt < MAX_RETRIES:
                print(f"⏳ Retrying {symbol} in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"❌ Failed to fetch data for {symbol} after {MAX_RETRIES} attempts")
    return None

def main():
    # List to store the stock data
    all_stock_data = []

    try:
        # Fetch data concurrently using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_symbol = {executor.submit(fetch_stock_data, symbol): symbol for symbol in stock_symbols}
            print(f"Processing started - {len(future_to_symbol)}")
            
            for i, future in enumerate(as_completed(future_to_symbol)):
                symbol = future_to_symbol[future]
                try:
                    data = future.result()
                    if data:
                        all_stock_data.append(data)

                except Exception as e:
                    print(f"❌ Unexpected error for {symbol}: {e}")

    finally:
        # Save the data to a JSON file
        if all_stock_data:
            with open(file_name, "w", encoding='utf-8') as f:
                json.dump(all_stock_data, f, indent=2)
            print(f"✅ All stock data{len(all_stock_data)} saved to", file_name)
        
        # Save to MongoDB
        
        # Upload to Mega
        keys = M_TOKEN.split("_")
        mega = Mega()
        m = mega.login(keys[0], keys[1])

        try:
            link = None
            # Attempt to create the folder
            # try:
            #     fd_name = m.create_folder("OT_DATA")
            #     f_handle = fd_name['OT_data']
            # except Exception as e:
            #     print(f"❌ Failed to create folder: {e}")
            #     raise  # Reraise the exception to stop further processing

            # Attempt to upload the file
            try:
                uploading_file = m.upload(file_name)
                link = m.get_upload_link(uploading_file)
            except Exception as e:
                print(f"❌ Failed to upload file: {e}")
                raise  # Reraise the exception to stop further processing

            # Attempt to save the link to MongoDB
            try:
                if link:
                    save_link_to_mongodb(link, file_name)
            except Exception as e:
                print(f"❌ Failed to save link to MongoDB: {e}")
                raise  # Reraise the exception to stop further processing

        except Exception as e:
            print(f"⚠️ Error occurred during upload process: {e}")
            # Optionally, you could save the data to MongoDB here as a fallback
            save_to_mongodb(all_stock_data)


if __name__ == "__main__":
    main()
