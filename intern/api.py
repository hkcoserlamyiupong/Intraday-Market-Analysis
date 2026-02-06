# api.py
from fastapi import FastAPI, Query
from typing import List, Optional
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient

app = FastAPI()

# Connections
#pg_engine = create_engine("postgresql://postgres:password@localhost:5432/market_data")
pg_engine = create_engine("postgresql://postgres:mysecretpassword@localhost:5433/market_data")
mongo_client = MongoClient("mongodb://localhost:27017")
mongo_coll = mongo_client["market_data"]["bars_1min"]

@app.get("/data")
def get_intraday_data(
    start_time: str,
    end_time: str,
    tickers: List[str] = Query(...),
    fields: List[str] = Query(["open","high","low","close","volume"]),
    source: str = Query("timescaledb", description="timescaledb or mongodb")
):
    start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))

    #allowed_fields = {"open","high","low","close","volume","vwap","trades","timestamp","ticker"}
    allowed_fields = {"open", "high", "low", "close", "volume", "timestamp", "ticker"}

    if source == "timescaledb":
        # Generate IN clause placeholders (e.g., :ticker1, :ticker2, ...)
        in_placeholders = ', '.join(f':ticker{i + 1}' for i in range(len(tickers)))

        # query = f"""
        #     SELECT timestamp, ticker, {', '.join([f for f in fields if f in allowed_fields])}
        #     FROM bars_1min
        #     WHERE ticker = ANY(:tickers)
        #     AND timestamp BETWEEN :start AND :end
        #     ORDER BY ticker, timestamp
        # """
        query = f"""
                SELECT timestamp, ticker, {', '.join([f for f in fields if f in allowed_fields])}
                FROM bars_1min 
                WHERE ticker IN ({in_placeholders})
                AND timestamp BETWEEN :start AND :end
                ORDER BY ticker, timestamp
            """

        #df = pd.read_sql(query, pg_engine, params={"tickers": tickers, "start": start_dt, "end": end_dt})
        # Create params dict with individual ticker bindings
        params = {f'ticker{i + 1}': ticker for i, ticker in enumerate(tickers)}
        params.update({"start": start_dt, "end": end_dt})
        df = pd.read_sql(text(query), pg_engine, params=params)
        return df.to_dict(orient="records")

    elif source == "mongodb":
        projection = {f: 1 for f in fields}
        projection["_id"] = 0
        projection["timestamp"] = 1
        projection["ticker"] = 1

        cursor = mongo_coll.find(
            {"ticker": {"$in": tickers}, "timestamp": {"$gte": start_dt, "$lte": end_dt}},
            projection
        ).sort([("ticker", 1), ("timestamp", 1)])

        return list(cursor)