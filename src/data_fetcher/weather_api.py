import sys
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from sqlalchemy import func

# pyrefly: ignore [missing-import]
from src.database.database import SessionLocal
# pyrefly: ignore [missing-import]
from src.database.models import Location, WeatherHistory

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def get_latest_timestamp(db, location_id: int) -> datetime:
    """Gets the most recent timestamp for a given location, or a default start date."""
    latest = db.query(func.max(WeatherHistory.timestamp)).filter(
        WeatherHistory.location_id == location_id
    ).scalar()
    
    if latest:
        return latest
    
    # Default: fetch the last 30 days if no data is present
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)

def fetch_weather_history():
    print("Starting incremental weather history fetch...")
    db = SessionLocal()
    
    try:
        locations = db.query(Location).all()
        if not locations:
            print("No locations found in the database. Please seed the database first.")
            return

        print(f"Found {len(locations)} locations.")
        
        # We process locations in batches of 10 to avoid URI too long errors
        batch_size = 10
        total_inserted = 0
        
        for i in range(0, len(locations), batch_size):
            batch = locations[i:i+batch_size]
            
            lats = [loc.latitude for loc in batch]
            lons = [loc.longitude for loc in batch]
            
            # Find the oldest needed start date in this batch
            # Note: For simplicity, we query the exact required range per batch based on the MIN of latest_timestamps
            # To be 100% efficient, we should query per-location, but since they usually update together, batching the earliest date is fine.
            latest_dates = [get_latest_timestamp(db, loc.id) for loc in batch]
            start_date = min(latest_dates).strftime("%Y-%m-%d")
            
            # The archive API is usually delayed by 2 days, but openmeteo handles this if we use the forecast API for recent days
            # However, since we are fetching historical data, we use the archive API up to yesterday.
            end_date = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
            
            # If start_date > end_date, we already have everything up to 2 days ago
            if start_date > end_date:
                print(f"Batch {i//batch_size + 1} is already up to date ({start_date}). Skipping.")
                continue
                
            print(f"Fetching batch {i//batch_size + 1} from {start_date} to {end_date} for {len(batch)} locations...")
            
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": lats,
                "longitude": lons,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": ["temperature_2m", "wind_speed_100m", "sunshine_duration", "global_tilted_irradiance"],
                "tilt": 35,
                "timezone": "UTC"
            }
            
            # Note: The free API allows up to 10000 requests per day. Batching coordinates counts as multiple locations.
            responses = openmeteo.weather_api(url, params=params)
            
            # Ensure we iterate correctly
            if not isinstance(responses, list):
                responses = [responses]
            
            # Process each response mapping back to the location
            for loc, response in zip(batch, responses):
                hourly = response.Hourly()
                
                # Times
                start = pd.to_datetime(hourly.Time(), unit="s", utc=True)
                end = pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True)
                freq = pd.Timedelta(seconds=hourly.Interval())
                time_arr = pd.date_range(start, end, freq=freq, inclusive="left")
                
                # Variables
                temp = hourly.Variables(0).ValuesAsNumpy()
                wind = hourly.Variables(1).ValuesAsNumpy()
                sun = hourly.Variables(2).ValuesAsNumpy()
                gti = hourly.Variables(3).ValuesAsNumpy()
                
                df = pd.DataFrame(data={
                    "timestamp": time_arr.tz_localize(None), # Store as naive UTC
                    "temperature_2m": temp,
                    "wind_speed_100m": wind,
                    "sunshine_duration": sun,
                    "global_tilted_irradiance": gti
                })
                
                # Filter out records we already have for this location
                latest_ts = latest_dates[batch.index(loc)]
                df = df[df["timestamp"] > latest_ts]
                
                if df.empty:
                    continue
                    
                # Insert into DB
                # Fastest way to insert is converting to dict and using bulk_insert_mappings or df.to_sql
                # We will use df.to_sql via pandas, but we need the engine.
                df["location_id"] = loc.id
                df.to_sql("weather_history", con=db.get_bind(), if_exists="append", index=False)
                
                total_inserted += len(df)
            
            # Sleep slightly to be polite to the free API (although batching already reduces requests)
            time.sleep(1)

        print(f"Finished! Inserted {total_inserted} new weather records.")
    
    except Exception as e:
        print(f"Error fetching weather data: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    fetch_weather_history()
