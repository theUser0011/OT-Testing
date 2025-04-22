import time
import os
import traceback
import csv
import json
from datetime import datetime
import pytz

from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager


mongo_url = os.getenv("MONGO_URL")


# Set download folder
down_folder = 'downloads'
download_dir = os.path.abspath(down_folder)
os.makedirs(download_dir, exist_ok=True)


# Path to ChromeDriver
chromedriver_path = r"chromedriver"

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
        "Open": round(open_price, 2),
        "Yesterday_High": round(yesterday_high, 2),
        "Yesterday_Low": round(yesterday_low, 2),
        "Buy_Entry": round(buy_entry, 2),
        "Sell_Entry": round(sell_entry, 2),
        "Buy_Stoploss": round(buy_stoploss, 2),
        "Sell_Stoploss": round(sell_stoploss, 2),
        "Shares_Buy": int(shares_num),
        "Shares_Sell": int(shares_num),
        "Signal":None,
        "Current_price":None
    }

    return output

def driver_initialize():
    options = Options()
    options.add_argument("--headless")  # Headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Set prefs for download behavior
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    # Options to avoid automation detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-notifications")
    options.add_argument("start-maximized")
    
    # Override the navigator object to mask automation properties
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--remote-debugging-port=9222')

    # Add user-agent to avoid detection
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")

    # Additional Chrome flags to improve automation evasion
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")

    service = Service(executable_path=chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def save_to_mongodb(data):
    try:
        client = MongoClient(mongo_url)
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

def convert_csv_to_json(input_csv_path):
    base_name = os.path.splitext(os.path.basename(input_csv_path))[0]
    dir_name = os.path.dirname(input_csv_path)
    json_file_path = os.path.join(dir_name, f"{base_name}.json")

    data = []
    with open(input_csv_path, mode='r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            data.append(row)

    for obj in data:
        open_val = obj['Open'] 
        high = obj['High'] 
        low = obj['Low']
        result_obj = calculate_and_save(open_val, high, low)
        obj['data'] = result_obj

    with open("data.json", "w", encoding='utf-8') as f:
        json.dump(data, f, indent=4)

    save_to_mongodb(data)
    return None

def main():
    driver = driver_initialize()
    try:
        driver.get("https://www.nseindia.com/market-data/top-gainers-losers")
        time.sleep(2)

        # Accept cookies
        try:
            accept_btn = driver.find_element(By.ID, "onetrust-accept-btn-handler")
            accept_btn.click()
            print("Cookies accepted.")
            time.sleep(2)
        except:
            print("No cookie popup found.")

        # Select NIFTY 50
        select_element = driver.find_element(By.ID, "index0")
        select = Select(select_element)
        select.select_by_value("NIFTY")
        print("Selected NIFTY 50 from dropdown.")
        time.sleep(3)

        # Click the download button
        download_button = driver.find_element(By.CLASS_NAME, "xlsdownload")
        actions = ActionChains(driver)
        actions.move_to_element(download_button).click().perform()
        print("Download button clicked.")
        time.sleep(3)
        print("Stopping driver..")
        driver.quit()
        # Get the latest CSV file
        csv_files = [f for f in os.listdir(down_folder) if f.endswith('.csv')]
        if csv_files:
            latest_csv = max([os.path.join(down_folder, f) for f in csv_files], key=os.path.getctime)
            convert_csv_to_json(latest_csv)
        else:
            print("❌ No CSV file found in downloads folder.")

    except Exception as e:
        print("Error occurred:", e)
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
