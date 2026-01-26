from fastapi import FastAPI, Query
from pymongo import MongoClient
from typing import List
from datetime import datetime

app = FastAPI()

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['sse_stocks']
collection = db['daily_data']

@app.get("/data")
def get_data(
    ticker: str = Query(..., description="Stock ticker, e.g., 600000"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    fields: List[str] = Query(..., description="Comma-separated fields, e.g., high,low,volume")
):
    # Validate dates
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD."}

    if start_dt > end_dt:
        return {"error": "Start date must be before end date."}

    # Query MongoDB
    query = {
        "ticker": ticker,
        "date": {"$gte": start_date, "$lte": end_date}
    }
    # Build projection
    projection = {"_id": 0, "date": 1, "ticker": 1}  # Always include date and ticker
    for field in fields:
        if field in ["open", "high", "low", "close", "volume"]:
            projection[field] = 1
        else:
            return {"error": f"Invalid field: {field}. Allowed: open,high,low,close,volume"}

    data = list(collection.find(query, projection).sort("date", 1))

    if not data:
        return {"message": "No data found for the given parameters."}

    return data