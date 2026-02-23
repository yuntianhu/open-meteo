from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

import pandas as pd
import requests

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

path = "location.csv"
#open 10 theads in the threadpool to make the machine run concurrently 
max_thread = 10 

# declare the varilbe to help the api call limit problem 

min_interval_s = 0.0   
max_call_miuntes = 600
max_call_hour=5000
max_call_daily = 10000

miuntes_call_number=0
hourly_call_number=0
daily_call_number=0

call_number=0 

# start sessions 
_session = requests.Session()
_api_lock = threading.Lock()
_last_call_ts = 0.0

#start timer
miuntes_timer=time.time()
hours_timer=time.time()
daily_timer=time.time()

#make it dataclass and frozen so this script works for data purpose and froze the data after they are fetched
@dataclass(frozen=True)
class Location:
    name: str
    region: str
    latitude: float
    longitude: float


def read_locations(csv_path: Path) -> List[Location]:
    df = pd.read_csv(csv_path) 
    required = {"name", "region", "latitude", "longitude"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"locations.csv missing columns: {sorted(missing)}")

    out: List[Location] = []
    for _, r in df.iterrows():
        out.append(
            Location(
                name=str(r["name"]).strip(),
                region=str(r["region"]).strip(),
                latitude=float(r["latitude"]),
                longitude=float(r["longitude"]),
            )
        )
    if not out:
        raise ValueError("locations.csv is empty")
    return out


def fetch_daily_precip_sum(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    timezone: str = "auto",
    timeout_s: int = 20,
) -> Dict[str, Any]:
    
    global _last_call_ts, call_number
    global miuntes_timer, hours_timer, daily_timer
    global miuntes_call_number, hourly_call_number, daily_call_number

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "precipitation_sum",
        "timezone": timezone,
    }

   
    with _api_lock:
        if min_interval_s > 0:
            now = time.time()
            wait = min_interval_s - (now - _last_call_ts)
            if wait > 0:
                time.sleep(wait)
            _last_call_ts = time.time()
        miuntes_call_number += 1
        hourly_call_number += 1
        daily_call_number+= 1
       
        if  time.time()- miuntes_timer >= 60 and miuntes_call_number >= 600:
            time.sleep(60)
            print("you call more than api limit. we have to wait for a miunte")
            miuntes_timer = time.time()
            miuntes_call_number =1

        if  time.time()-hours_timer >= 3600 and call_number >= 5000:
            time.sleep(3600)
            print("you call more than api limit. we have to wait for five miuntes")
            hours_timer = time.time()
            hourly_call_number=1

        if  time.time()-daily_timer >= 86400 and call_number >= 10000:
            raise SystemExit("you call more than api limit. we have to wait for five miuntes")
           

        resp = _session.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=timeout_s)
        print(resp.json())
        code=resp.status_code

        if code == 429:
            print("429: you call more than api limit. we have to wait for five miuntes")
            time.sleep(300)
            resp = _session.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=timeout_s)
        elif code == 404:
            raise SystemExit ("404: please check your API endpoint")
        elif code == 500:
            raise SystemExit("500: please contact vendor, you have serverside prblem")
   
       
        call_number +=1
        if call_number % 10 ==0:
            time.sleep(1)

        print(call_number)


    resp.raise_for_status()
    data = resp.json()

    if (
        "daily" not in data
        or "time" not in data["daily"]
        or "precipitation_sum" not in data["daily"]
    ):
        raise RuntimeError("Unexpected API response: missing daily.time or daily.precipitation_sum")

    return data


def summarize_rainy_days_for_location_year(
    loc: Location,
    year: int,
    threshold_mm: float = 1.0,
    timezone: str = "auto",
) -> Dict[str, Any]:
    data = fetch_daily_precip_sum(
        lat=loc.latitude,
        lon=loc.longitude,
        start_date=f"{year}-01-01",
        end_date=f"{year}-12-31",
        timezone=timezone,
    )

    daily = data["daily"]
    df = pd.DataFrame(
        {
            "date": daily["time"],
            "precipitation_sum": daily["precipitation_sum"],
        }
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()

    is_rainy = df["precipitation_sum"] > threshold_mm

    rainy_days = int(is_rainy.sum())
    days = int(len(df))

    return {
        "region": loc.region,
        "location": loc.name,
        "year": year,
        "rainy_days": rainy_days,
        "days": days,
        "rainy_pct": (rainy_days / days) if days else 0.0,
    }


def _task(loc: Location, year: int, threshold_mm: float, timezone: str) -> Dict[str, Any]:
    return summarize_rainy_days_for_location_year(
        loc=loc,
        year=year,
        threshold_mm=threshold_mm,
        timezone=timezone,
    )


def build_rainy_day_summary(
    locations_csv: Path,
    start_year: int = 2024,
    end_year: int = 2025,
    threshold_mm: float = 1.0,
    timezone: str = "auto",
) -> pd.DataFrame:
    locations = read_locations(locations_csv)

    tasks: List[Tuple[Location, int]] = [
        (loc, year) for loc in locations for year in range(start_year, end_year + 1)
    ]
    total_calls = len(tasks)

    rows: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_thread) as executor:
        futures = {
            executor.submit(_task, loc, year, threshold_mm, timezone): (loc, year)
            for loc, year in tasks
        }

        done = 0
        for fut in as_completed(futures):
            loc, year = futures[fut]
            done += 1
            try:
                rows.append(fut.result())
            except Exception as e:
                rows.append({
                    "region": loc.region,
                    "location": loc.name,
                    "year": year,
                    "rainy_days": None,
                    "days": None,
                    "rainy_pct": None,
                    "error": str(e),
                })

            if done % 10 == 0 or done == total_calls:
                print(f"Progress: {done}/{total_calls}")

    return pd.DataFrame(rows)


if __name__ == "__main__":
    summary = build_rainy_day_summary(
        locations_csv=Path("location.csv"),
        start_year=2016,
        end_year=2025,
        threshold_mm=1.0,
        timezone="auto",
    )
    print(summary)
    summary.to_csv("out_rainy_days_summary_1.csv", index=False)
    print("Wrote out_rainy_days_summary.csv")
