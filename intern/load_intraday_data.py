import gdown
import zipfile
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from datetime import datetime
import time

# ========================= CONFIG =========================
FOLDER_ID = "1seVNVpLjwKeWysR5KZa3RqZUS2EotTMu"
DOWNLOAD_DIR = Path("intraday_data")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Database connections
#POSTGRES_URI = "postgresql://postgres:mysecretpassword@127.0.0.1:5432/market_data"
#docker stop timescaledbdocker rm timescaledbdocker run -d --name timescaledb -p 5433:5432 -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_USER=postgres -e POSTGRES_DB=market_data timescale/timescaledb:latest-pg16
POSTGRES_URI = "postgresql://postgres:mysecretpassword@localhost:5433/market_data"

MONGO_URI = "mongodb://127.0.0.1:27017"


# =========================================================

# def download_folder():
#     print("Downloading folder from Google Drive...")
#     url = f"https://drive.google.com/drive/folders/{FOLDER_ID}"
#     gdown.download_folder(url, output=str(DOWNLOAD_DIR), quiet=False, remaining_ok=True)
#     print(f"✅ Download complete. Files saved in {DOWNLOAD_DIR}")


# def extract_zips():
#     print("\nExtracting .csv.zip files...")
#     for zip_file in DOWNLOAD_DIR.glob("*.zip"):
#         with zipfile.ZipFile(zip_file, 'r') as zip_ref:
#             zip_ref.extractall(DOWNLOAD_DIR)
#         print(f"   Extracted: {zip_file.name}")
#         zip_file.unlink()  # optional: delete zip after extraction


def inspect_data():
    csv_files = list(DOWNLOAD_DIR.glob("*.csv"))
    if not csv_files:
        print("No CSV files found!")
        return None

    sample_file = csv_files[0]
    print(f"\nInspecting sample file: {sample_file.name}")
    df_sample = pd.read_csv(sample_file, nrows=10)
    print("Columns:", df_sample.columns.tolist())
    print("Shape:", df_sample.shape)
    print("First timestamp:", df_sample.iloc[0, 0] if 'timestamp' in df_sample.columns else "N/A")
    print("Sample rows:\n", df_sample.head(3))
    return df_sample.columns.tolist()


def load_to_timescaledb():
    print("\nLoading data into TimescaleDB...")
    engine = create_engine(POSTGRES_URI)

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS bars_1min"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))

    csv_files = list(DOWNLOAD_DIR.glob("*.csv"))
    for i, csv_file in enumerate(csv_files, 1):
        print(f"  [{i}/{len(csv_files)}] Loading {csv_file.name}")
        df = pd.read_csv(csv_file)

        # Standardize column names
        df.columns = [col.strip().lower() for col in df.columns]
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'timestamp'})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Add ticker column from filename stem
        df['ticker'] = csv_file.stem

        # Load to PostgreSQL - use "replace" for first file to create table schema
        # df.to_sql("bars_1min", engine, if_exists="append", index=False, chunksize=50000)
        if_exists_mode = "replace" if i == 1 else "append"
        df.to_sql("bars_1min", engine, if_exists=if_exists_mode, index=False, chunksize=50000)

        # Create hypertable on first file
        if i == 1:
            with engine.connect() as conn:
                conn.execute(text("""
                    SELECT create_hypertable('bars_1min', 'timestamp', 
                        chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE, migrate_data => TRUE);
                """))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ticker_time ON bars_1min (ticker, timestamp DESC);"))

    print("✅ TimescaleDB ingestion complete")


def load_to_mongodb():
    print("\nLoading data into MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client["market_data"]
    collection = db["bars_1min"]
    collection.drop()  # clear previous data

    csv_files = list(DOWNLOAD_DIR.glob("*.csv"))
    #for csv_file in csv_files:
    for i, csv_file in enumerate(csv_files, 1):
        print(f"  [{i}/{len(csv_files)}] Loading {csv_file.name}")
        df = pd.read_csv(csv_file)
        df.columns = [col.strip().lower() for col in df.columns]
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'timestamp'})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Add ticker column from filename stem
        df['ticker'] = csv_file.stem

        records = df.to_dict(orient='records')
        collection.insert_many(records, ordered=False)

    collection.create_index([("ticker", 1), ("timestamp", 1)])
    print("✅ MongoDB ingestion complete")


# ======================== RUN ALL ========================
if __name__ == "__main__":
    start_time = time.time()

    #download_folder()
    #extract_zips()
    columns = inspect_data()

    if columns:
        load_to_timescaledb()
        load_to_mongodb()

    print(f"\nTotal time: {(time.time() - start_time) / 60:.1f} minutes")