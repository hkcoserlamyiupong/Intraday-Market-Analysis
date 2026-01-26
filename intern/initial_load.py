#pip install yfinance akshare pymongo fastapi uvicorn pandas openpyxl
import akshare as ak
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from datetime import datetime
import time

# ========================= CONFIG =========================
MONGO_URI = "mongodb://127.0.0.1:27017/"          # Change if using Atlas
DB_NAME = "sse_stocks"
COLLECTION_NAME = "daily_data"
START_DATE = "2010-01-01"
# =========================================================
# Connect to MongoDB (update connection string if needed)
#client = MongoClient('mongodb://localhost:27017/')
client = MongoClient(MONGO_URI)
db = client[DB_NAME] #db = client['sse_stocks']
collection = db[COLLECTION_NAME] #collection = db['daily_data']
# 測試操作
print(db.list_collection_names())

# Create unique index
#collection.create_index([('ticker', 1), ('date', 1)], unique=True)
# Drop old indexes and create correct unique index
collection.drop_indexes()
collection.create_index([("ticker", 1), ("date", 1)], unique=True)
print("✅ Index on (ticker, date) created.")

import yfinance as yf

# Fetch SSE tickers from HKEX Shanghai Connect list
print("Downloading SSE eligible stocks from HKEX...")

# Get list of SSE tickers (Main Board A + STAR Market)
# import akshare as ak
# main_board = ak.stock_info_sh_name_code(symbol="主板A股")
# star_board = ak.stock_info_sh_name_code(symbol="科创板")
# print(main_board)

#all_tickers_df = pd.concat([main_board, star_board])
#tickers = all_tickers_df['symbol'].tolist()  # e.g., ['600000', '600001', ...]
#tickers = all_tickers_df['证券代码'].tolist()

# Fetch SSE tickers from HKEX Shanghai Connect (eligible stocks)
url = "https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Eligible-Stocks/View-All-Eligible-Securities_xls/SSE_Securities.xls"

# try:
#     tickers_df = pd.read_excel(url, skiprows=1)
#     tickers = (
#         tickers_df["Stock Code"]
#         .astype(str)
#         .str.zfill(6)
#         .drop_duplicates()
#         .tolist()
#     )
#     print(f"✅ Found {len(tickers)} SSE stocks.")
# except Exception as e:
#     print(f"❌ Failed to download ticker list: {e}")
#     print("Please check the HKEX URL or internet connection.")
#     exit()

tickers_df = pd.read_excel(url, skiprows=4)  # Skip title row
#tickers_df.head()
tickers = tickers_df['SSE Stock Code'].astype(str).str.zfill(6).unique().tolist()  # 6-digit codes

#print(tickers)

# start_date = "20100101"
# end_date = datetime.now().strftime("%Y%m%d")
start_date = '2010-01-01'
# Current end date
end_date = datetime.now().strftime("%Y-%m-%d")

# ======================= MAIN LOOP =======================
#for ticker in tickers:
for i, ticker in enumerate(tickers, 1):
    yahoo_ticker = f"{ticker}.SS"  # '688981.SS'
    print(f"[{i}/{len(tickers)}] Processing {ticker} ({yahoo_ticker})...")
    try:
        # df = ak.stock_zh_a_hist(
        #     symbol=ticker,
        #     period="daily",
        #     start_date=start_date,
        #     end_date=end_date,
        #     adjust=""  # No adjustment; raw data
        # )
        # df = yf.download(yahoo_ticker, start=start_date, end=end_date, progress=False)
        #df = yf.download(yahoo_ticker, start=start_date, end=end_date, progress=False, multi_level_index=False)
        df = yf.download(
            yahoo_ticker,
            start=START_DATE,
            end=end_date,
            progress=False,
            auto_adjust=False,
            threads=False,
            multi_level_index=False
        )
        if df.empty:
            print(f"No data for {ticker}")
            continue

        df = df.reset_index()  # Reset the index of the DataFrame, and use the default one instead # ensures 'Date' column exists

        # Rename columns to match schema
        # df = df.rename(columns={
        #     '日期': 'date',
        #     '开盘': 'open',
        #     '最高': 'high',
        #     '最低': 'low',
        #     '收盘': 'close',
        #     '成交量': 'volume'
        # })
        # Standardize column names
        df = df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })

        # Select relevant columns and add ticker
        df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
        #df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df['ticker'] = ticker
        #df['Date'] = df['Date'].astype(str)  # Ensure str format
        #df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df['date'] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        # Ensure volume is integer
        #df['volume'] = df['volume'].astype(int)
        df["volume"] = df["volume"].fillna(0).astype(int)

        # Insert to MongoDB (bulk insert)
        records = df.to_dict('records')
        # Debug: print first record
        if records:
            print(f"   Sample: {records[0]}")
            # Insert with error handling
            try:
                result = collection.insert_many(records, ordered=False)
                print(f"✅ Inserted {len(result.inserted_ids)} records for {ticker}")
            #except Exception as e:
                #print(f"⚠️ Partial insert error for {ticker}: {e}")
            except BulkWriteError as bwe:
                inserted = len(bwe.details.get("nInserted", 0))
                dup_errors = len(bwe.details.get("writeErrors", []))
                print(f"   ⚠️ Inserted {inserted}, skipped {dup_errors} duplicates (already exist)")
            #collection.insert_many(records, ordered=False)
        print(f"Inserted data for {ticker}")


        time.sleep(1)  # Avoid rate limits
    except Exception as e:
        #print(f"Error for {ticker}: {e}")
        print(f"   ❌ Error processing {ticker}: {type(e).__name__} - {e}")

print("Initial load complete.")