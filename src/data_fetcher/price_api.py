import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from sqlalchemy import func

# pyrefly: ignore [missing-import]
from src.database.database import SessionLocal
# pyrefly: ignore [missing-import]
from src.database.models import ElectricityPrice

BASE_URL = "https://api.energy-charts.info"

def get_latest_price_timestamp(db) -> datetime:
    """Gets the most recent timestamp for electricity prices."""
    latest = db.query(func.max(ElectricityPrice.timestamp)).scalar()
    
    if latest:
        return latest
    
    # Default: fetch the last 30 days if no data is present
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)

def fetch_electricity_prices():
    print("Starting incremental electricity price fetch...")
    db = SessionLocal()
    
    try:
        latest_ts = get_latest_price_timestamp(db)
        
        # Start date is the day of our latest timestamp
        start_date = latest_ts.strftime("%Y-%m-%d")
        # End date is today
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # If we already fetched up to today, we might still want to fetch today to get new hours,
        # but the API allows fetching the same date range and we just filter duplicates out.
        
        print(f"Fetching electricity prices from {start_date} to {end_date}...")
        
        url = f"{BASE_URL}/price?bzn=DE-LU&start={start_date}&end={end_date}"
        
        response = requests.get(url)
        if response.status_code != 200:
            print(f"API Request failed with status code {response.status_code}")
            return
            
        data = response.json()
        
        if "unix_seconds" not in data or "price" not in data:
            print("Invalid response format from Energy-Charts.")
            return
            
        df = pd.DataFrame({
            "unix_seconds": data["unix_seconds"],
            "price": data["price"]
        })
        
        # Convert unix seconds to UTC datetime
        df["timestamp"] = pd.to_datetime(df["unix_seconds"], unit="s", utc=True).dt.tz_localize(None)
        df.drop(columns=["unix_seconds"], inplace=True)
        
        # The API sometimes returns 15-minute intervals. Let's resample to hourly mean.
        df.set_index("timestamp", inplace=True)
        df = df.resample("1h").mean().reset_index()
        
        # Filter only records newer than what we already have
        df = df[df["timestamp"] > latest_ts]
        
        if df.empty:
            print("No new price data available.")
            return
            
        # Insert into DB
        print(f"Inserting {len(df)} new price records...")
        df.to_sql("electricity_prices", con=db.get_bind(), if_exists="append", index=False)
        print("Finished!")
        
    except Exception as e:
        print(f"Error fetching price data: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    fetch_electricity_prices()
