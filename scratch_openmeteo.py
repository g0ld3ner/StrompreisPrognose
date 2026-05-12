import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
import datetime

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Let's try the ensemble API which often has past forecasts
url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
params = {
    "latitude": 52.52,
    "longitude": 13.41,
    "start_date": "2024-05-01",
    "end_date": "2024-05-08", # 7 days
    "hourly": ["temperature_2m"],
    "models": "icon_global",
    "match_time": "2024-05-01T00:00" # trying to match reference time
}

try:
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()
    print("Success. Length:", len(hourly.Variables(0).ValuesAsNumpy()))
    
except Exception as e:
    print("Error:", e)

