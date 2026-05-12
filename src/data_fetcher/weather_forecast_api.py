import sys
import time
from datetime import datetime, timezone
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

# pyrefly: ignore [missing-import]
from src.database.database import SessionLocal
# pyrefly: ignore [missing-import]
from src.database.models import Location, WeatherForecast

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# The variables we want to fetch
BASE_VARIABLES = [
    "temperature_2m",
    "wind_speed_100m",
    "sunshine_duration",
    "global_tilted_irradiance"
]

def fetch_historical_forecasts(db, locations, past_days=400):
    """
    Fetches the 2D forecast matrix for the past `past_days` using the Previous Runs API.
    It reconstructs the 168-hour forecast generated at 00:00 UTC for each past day.
    """
    print(f"Starting historical backfill for the last {past_days} days using Previous Runs API...")
    
    # We batch locations to avoid URL too long errors
    batch_size = 15
    total_inserted = 0
    
    # We need previous_day1 to previous_day7 for all variables
    hourly_vars = []
    for var in BASE_VARIABLES:
        for i in range(1, 8):
            hourly_vars.append(f"{var}_previous_day{i}")
            
    url = "https://previous-runs-api.open-meteo.com/v1/forecast"
    
    for i in range(0, len(locations), batch_size):
        batch = locations[i:i+batch_size]
        lats = [loc.latitude for loc in batch]
        lons = [loc.longitude for loc in batch]
        
        print(f"Fetching historical batch {i//batch_size + 1}/{(len(locations)-1)//batch_size + 1} ({len(batch)} locations)...")
        
        params = {
            "latitude": lats,
            "longitude": lons,
            "past_days": past_days,
            "forecast_days": 1,
            "hourly": hourly_vars,
            "tilt": 35,
            "timezone": "UTC"
        }
        
        try:
            responses = openmeteo.weather_api(url, params=params)
        except Exception as e:
            print(f"Error fetching batch: {e}")
            continue
            
        if not isinstance(responses, list):
            responses = [responses]
            
        for loc, response in zip(batch, responses):
            hourly = response.Hourly()
            
            # Extract time array
            start = pd.to_datetime(hourly.Time(), unit="s", utc=True)
            end = pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True)
            freq = pd.Timedelta(seconds=hourly.Interval())
            time_arr = pd.date_range(start, end, freq=freq, inclusive="left").tz_localize(None)
            
            # Build initial DataFrame
            data = {"timestamp": time_arr}
            var_idx = 0
            for var in BASE_VARIABLES:
                for d in range(1, 8):
                    col_name = f"{var}_prev{d}"
                    data[col_name] = hourly.Variables(var_idx).ValuesAsNumpy()
                    var_idx += 1
            
            df = pd.DataFrame(data)
            
            # Melt and reshape to reconstruct the 2D matrix
            # df_melted will have columns: timestamp, variable_prev (e.g. temperature_2m_prev1), value
            df_melted = df.melt(id_vars=["timestamp"], var_name="variable_prev", value_name="value")
            
            # Extract base variable and prev_days using vectorized string operations for speed
            # e.g. "temperature_2m_prev1" -> base_var="temperature_2m", prev_days=1
            extracted = df_melted["variable_prev"].str.extract(r"(.+)_prev(\d+)")
            df_melted["base_var"] = extracted[0]
            df_melted["prev_days"] = extracted[1].astype(int)
            
            # The forecast for target `timestamp` from `prev_days` ago was generated on `timestamp - prev_days`
            # Since the daily run is considered 00:00 UTC, we floor the timestamp to the day.
            df_melted["forecast_generated_at"] = df_melted["timestamp"].dt.floor("D") - pd.to_timedelta(df_melted["prev_days"], unit="D")
            
            # Pivot back so each base_var is a column
            # We group by forecast_generated_at and timestamp
            df_final = df_melted.pivot_table(
                index=["forecast_generated_at", "timestamp"], 
                columns="base_var", 
                values="value"
            ).reset_index()
            
            # Add location_id
            df_final["location_id"] = loc.id
            
            # Drop NaN values if any (e.g. from the shifting at the edges)
            df_final = df_final.dropna(subset=BASE_VARIABLES, how='all')
            
            if df_final.empty:
                continue
                
            # Insert into database using chunking to avoid memory spikes
            try:
                df_final.to_sql("weather_forecast", con=db.get_bind(), if_exists="append", index=False, chunksize=10000)
                total_inserted += len(df_final)
            except Exception as e:
                # If there are duplicates, to_sql append will fail if it hits a unique constraint
                # We can use a slower method or just ignore and warn.
                print(f"Warning inserting for location {loc.id}: {e}")
                pass
                
        time.sleep(1) # Polite sleep
        
    print(f"Historical backfill complete. Inserted {total_inserted} records.")


def fetch_live_forecast(db, locations):
    """
    Fetches the live 168-hour forecast for the current day using the standard Forecast API.
    """
    print("Starting live forecast fetch for the next 7 days...")
    
    batch_size = 15
    total_inserted = 0
    url = "https://api.open-meteo.com/v1/forecast"
    
    # We define the reference time for this run as today's 00:00 UTC
    today_00 = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    
    for i in range(0, len(locations), batch_size):
        batch = locations[i:i+batch_size]
        lats = [loc.latitude for loc in batch]
        lons = [loc.longitude for loc in batch]
        
        print(f"Fetching live batch {i//batch_size + 1}/{(len(locations)-1)//batch_size + 1}...")
        
        params = {
            "latitude": lats,
            "longitude": lons,
            "past_days": 0,
            "forecast_days": 8, # We fetch 8 days to ensure we get a full 168h from the start of the next day if needed
            "hourly": BASE_VARIABLES,
            "tilt": 35,
            "timezone": "UTC"
        }
        
        try:
            responses = openmeteo.weather_api(url, params=params)
        except Exception as e:
            print(f"Error fetching batch: {e}")
            continue
            
        if not isinstance(responses, list):
            responses = [responses]
            
        for loc, response in zip(batch, responses):
            hourly = response.Hourly()
            
            start = pd.to_datetime(hourly.Time(), unit="s", utc=True)
            end = pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True)
            freq = pd.Timedelta(seconds=hourly.Interval())
            time_arr = pd.date_range(start, end, freq=freq, inclusive="left").tz_localize(None)
            
            data = {"timestamp": time_arr}
            for idx, var in enumerate(BASE_VARIABLES):
                data[var] = hourly.Variables(idx).ValuesAsNumpy()
                
            df = pd.DataFrame(data)
            df["forecast_generated_at"] = today_00
            df["location_id"] = loc.id
            
            # We only keep the 168 hours starting from tomorrow 00:00 UTC 
            # to match the historical backfill logic (which predicts D+1 to D+7)
            start_target = today_00 + pd.Timedelta(days=1)
            end_target = start_target + pd.Timedelta(days=7) # 7 days later
            
            df = df[(df["timestamp"] >= start_target) & (df["timestamp"] < end_target)]
            
            if df.empty:
                continue
                
            try:
                df.to_sql("weather_forecast", con=db.get_bind(), if_exists="append", index=False)
                total_inserted += len(df)
            except Exception as e:
                # If we already fetched today, it will throw a unique constraint error
                pass
                
        time.sleep(1)
        
    print(f"Live fetch complete. Inserted {total_inserted} records.")


def main():
    db = SessionLocal()
    try:
        locations = db.query(Location).all()
        if not locations:
            print("No locations found in the database. Please seed the database first.")
            return
            
        # Check if the weather_forecast table is empty
        # If it's empty, we run the backfill. Otherwise, we run the live fetch.
        count = db.query(WeatherForecast).count()
        if count == 0:
            fetch_historical_forecasts(db, locations, past_days=400)
        else:
            fetch_live_forecast(db, locations)
            
    finally:
        db.close()

if __name__ == "__main__":
    main()
