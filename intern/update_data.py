import yfinance as yf
import akshare as ak
import pandas as pd

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from datetime import datetime, timedelta
import time

# ========================= CONFIG =========================
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME = "sse_stocks"
COLLECTION_NAME = "daily_data"
# =========================================================

# Connect to MongoDB
#client = MongoClient('mongodb://localhost:27017/')
#db = client['sse_stocks']
#collection = db['daily_data']
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Ensure index exists
collection.create_index([("ticker", 1), ("date", 1)], unique=True)
print("✅ Index verified.")

# Get all unique tickers from DB (after initial load)
tickers = collection.distinct("ticker")
if not tickers:
    print("⚠️ No tickers found in database. Run initial_load.py first.")
    exit()

print(f"Found {len(tickers)} tickers in database. Starting daily update...")

#today = datetime.now().strftime("%Y%m%d")
end_date = datetime.now().strftime("%Y-%m-%d")

# ======================= MAIN UPDATE LOOP =======================
#for ticker in tickers:
for i, ticker in enumerate(tickers, 1):
    yahoo_ticker = f"{ticker}.SS"
    print(f"[{i}/{len(tickers)}] Updating {ticker} ({yahoo_ticker})...")
    try:
        # Find the latest date in DB for this ticker
        latest_doc = collection.find_one(
            {"ticker": ticker},
            sort=[("date", -1)],
            projection={"date": 1}
        )
        if not latest_doc:
            print(f"   ⚠️ No existing data for {ticker}. Skipping.")
            continue

        #start_date = (datetime.strptime(latest['date'], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y%m%d")
        latest_date = datetime.strptime(latest_doc["date"], "%Y-%m-%d")
        start_date = (latest_date + timedelta(days=1)).strftime("%Y-%m-%d")

        # if start_date > today:
        #     print(f"{ticker} already up to date.")
        #     continue
        if start_date > end_date:
            print(f"   ✅ Already up to date.")
            continue
        print(f"   Fetching data from {start_date} to {end_date}...")

        # df = ak.stock_zh_a_hist(
        #     symbol=ticker,
        #     period="daily",
        #     start_date=start_date,
        #     end_date=today,
        #     adjust=""
        # )
        df = yf.download(
            yahoo_ticker,
            start=start_date,
            end=end_date,
            progress=False,
            threads=False,
            multi_level_index=False
        )
        if df.empty or len(df) == 0:
            print(f"   ⚠️ No new trading data for {ticker} available yet (possible weekend or delayed update).")
            continue

        df = df.reset_index()

        # Standardize columns
        # Rename and prepare
        # df = df.rename(columns={
        #     '日期': 'date',
        #     '开盘': 'open',
        #     '最高': 'high',
        #     '最低': 'low',
        #     '收盘': 'close',
        #     '成交量': 'volume'
        # })
        df = df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        })
        # Add ticker and format date
        df["ticker"] = ticker
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d") #df['date'] = df['date'].astype(str)
        df["volume"] = df["volume"].fillna(0).astype(int)

        #df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
        # Select columns
        cols = ["date", "ticker", "open", "high", "low", "close", "volume"]
        if "adj_close" in df.columns:
            cols.append("adj_close")
        df = df[cols]

        # Insert new records
        records = df.to_dict('records')

        # Validation: prevent null ticker/date
        bad_records = [r for r in records if r.get("ticker") is None or r.get("date") is None]
        if bad_records:
            print(f"   ⚠️ Found {len(bad_records)} invalid records. Skipping.")
            continue

        # if records:
        #     collection.insert_many(records, ordered=False)
        # print(f"Updated {ticker} with {len(records)} new days.")

        # Insert with duplicate handling
        try:
            result = collection.insert_many(records, ordered=False)
            print(f"   ✅ Inserted {len(result.inserted_ids)} new records")
        except BulkWriteError as bwe:
            inserted = bwe.details.get("nInserted", 0)
            dup_count = len(bwe.details.get("writeErrors", []))
            print(f"   ⚠️ Inserted {inserted} records ({dup_count} duplicates skipped)")

        time.sleep(1)  # Avoid rate limits
    except Exception as e:
        print(f"Error updating {ticker}: {e}")

print("\n🎉 Daily update completed!")