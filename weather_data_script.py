import requests

API_KEY = "a8e820e2c02af8c004ed22d832a2b23d"
cities = ["Budapest", "Debrecen", "Sopron","Tokyo", "London", "Wien", "Hamburg"] # Szűrés bárosokra
all_data = []


import time
from datetime import datetime


#%% API call
for i in range(15):
    for city in cities:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url) # API hívás
        if response.status_code == 200:
            data = response.json()
            all_data.append(data)
        else:
            print("Hiba történt a lekéréskor")
    time.sleep(5)  # kis szünet a hívások között
  
#%% create DataFrame 
import pandas as pd

def flatten_weather_record(record: dict) -> dict:
    """
    Egy OpenWeatherMap current weather JSON-ból visszaad egy lapos dict-et,
    csak a számunkra fontos mezőkkel.
    """
    return {
        "city": record.get("name"),
        "temp_c": record.get("main", {}).get("temp"),
        "feels_like_c": record.get("main", {}).get("feels_like"),
        "humidity_pct": record.get("main", {}).get("humidity"),
        "wind_speed_ms": record.get("wind", {}).get("speed"),
        "weather_main": record.get("weather", [{}])[0].get("main"),
        "weather_desc": record.get("weather", [{}])[0].get("description"),
        "visibility_m": record.get("visibility"),
        "cloudiness_pct": record.get("all"),
        "data_calc_unix": record.get("dt"),  # OpenWeather szerinti epoch timestamp (nyers)
    }

# 1) all_data -> list[dict] sorokba konvertálás
flat_rows = [flatten_weather_record(rec) for rec in all_data]


df_raw = pd.DataFrame(flat_rows)

#%% DataFrame Analyze and ETL
df_raw.info() # Null -ok vizsgálata 
 
for i in df_raw.columns:
    if i != 'timestamp':
        print(i+ " : "+ str(set(df_raw[i]))) # Értékkészlet vizsgálat
        
df_new = df_raw.drop(columns=['cloudiness_pct']) # Mivel minden sorhoz None tartozott, így üres változót kidobtam


#Dátum változó konvertálás
df_raw["data_calc_utc"] = pd.to_datetime(df_raw["data_calc_unix"], unit="s", utc=True) # Időformátumba konvertálás
print(df_raw.data_calc_utc.head())

df_raw["data_calc_local"] = df_raw["data_calc_utc"].dt.tz_convert("Europe/Budapest") # Helyi időzónára állítás
print(df_raw.data_calc_local.hed())

# Hétvége indikátor kimutatásokhoz 
df_raw["is_weekend"] = pd.to_datetime(df_raw["data_calc_local"]).dt.day_name().isin(["Saturday", "Sunday"]).astype(int)


#%%
# Save to CSV
df_raw.to_csv("raw_weather.csv", index=False)

