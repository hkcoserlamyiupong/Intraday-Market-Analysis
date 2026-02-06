import time
import statistics
import requests
from tabulate import tabulate
from datetime import datetime

from pathlib import Path

# API Config (update if your API runs on a different port/host)
#API_URL = "http://localhost:8000/data"
API_URL = "http://127.0.0.1:8000/data"
DOWNLOAD_DIR = Path("intraday_data")

# Test Scenarios: List of dicts with query params
SCENARIOS = [
    {
        "name": "Single ticker, 1 day",
        "tickers": ["ACC"],  # Replace with actual tickers from your data
        "start_time": "2023-01-02T09:30:00+00:00",
        "end_time": "2023-01-02T16:00:00+00:00",
        "fields": ["open", "high", "low", "close", "volume"]
    },
    {
        "name": "Single ticker, 1 month",
        "tickers": ["ACC"],
        "start_time": "2023-01-01T00:00:00+00:00",
        "end_time": "2023-01-31T23:59:59+00:00",
        "fields": ["open", "high", "low", "close", "volume"]
    },
    {
        "name": "Single ticker, 1 year",
        "tickers": ["ACC"],
        "start_time": "2023-01-01T00:00:00+00:00",
        "end_time": "2023-12-31T23:59:59+00:00",
        "fields": ["open", "high", "low", "close", "volume"]
    },
    {
        "name": "10 tickers, 1 day",
        "tickers": ["ACC", "BOSCHLTD", "CHOLAFIN", "DABUR", "EICHERMOT", "GAIL", "HAVELLS", "ICICIBANK", "JINDALSTEL", "KOTAKBANK"],
        # Replace with 10 from your data
        "start_time": "2023-01-02T09:30:00+00:00",
        "end_time": "2023-01-02T16:00:00+00:00",
        "fields": ["open", "high", "low", "close", "volume"]
    },
    {
        "name": "all tickers, full range",
        #"tickers": ["ACC", "GOOGL", "MSFT", "AMZN", "TSLA"] * 10,  # Placeholder; use 50 unique from your data
        "tickers": list(DOWNLOAD_DIR.glob("*.csv")),
        "start_time": "2015-01-01T00:00:00+00:00",
        "end_time": datetime.now().isoformat(),
        "fields": ["open", "high", "low", "close", "volume"]
    }
]


def benchmark_query(source, tickers, start_time, end_time, fields, n=30):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        params = {
            "start_time": start_time,
            "end_time": end_time,
            "tickers": tickers,
            "fields": fields,
            "source": source  # "timescaledb" or "mongodb"
        }
        response = requests.get(API_URL, params=params)
        if response.status_code != 200:
            raise ValueError(f"API error: {response.text}")
        _ = response.json()  # Simulate processing the result
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000)  # ms
    return {
        "mean_ms": statistics.mean(times),
        "p95_ms": statistics.quantiles(times, n=20)[18],
        "min_ms": min(times),
        "max_ms": max(times)
    }


def run_benchmarks():
    results = []
    for scenario in SCENARIOS:
        print(f"\nRunning benchmark: {scenario['name']}")

        # TimescaleDB
        ts_result = benchmark_query(
            "timescaledb", scenario["tickers"], scenario["start_time"],
            scenario["end_time"], scenario["fields"]
        )

        # MongoDB
        mongo_result = benchmark_query(
            "mongodb", scenario["tickers"], scenario["start_time"],
            scenario["end_time"], scenario["fields"]
        )

        results.append({
            "Scenario": scenario["name"],
            "TimescaleDB Mean (ms)": ts_result["mean_ms"],
            "TimescaleDB P95 (ms)": ts_result["p95_ms"],
            "MongoDB Mean (ms)": mongo_result["mean_ms"],
            "MongoDB P95 (ms)": mongo_result["p95_ms"],
            "TimescaleDB Faster By (x)": mongo_result["mean_ms"] / ts_result["mean_ms"]
        })

    # Print comparison table
    print("\nPerformance Comparison:")
    print(tabulate(results, headers="keys", tablefmt="grid", floatfmt=".2f"))


if __name__ == "__main__":
    run_benchmarks()