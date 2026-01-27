import requests
import time
import pandas as pd
from datetime import datetime
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv('.env')
except Exception:
    pass

NOAA_TOKEN = os.getenv("NOAA_TOKEN")
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
HEADERS = {"token": NOAA_TOKEN} if NOAA_TOKEN else {}

DATASET_ID = "GHCND"
DATATYPES = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]  # Temp Max/Min, precipitation, snow depth & snowfall

REQUEST_SLEEP = 1.5
ERROR_BACKOFF_EXTRA = 5.0
MAX_LIMIT = 1000

# STATIONS_FILE contains list of stations to grab data for. 
# use fetch stations to get list to decide which ones you want
STATIONS_FILE = "airport-list.txt"
OUTPUT_DIR = "weather-data"

def noaa_get(endpoint, params):
    """Wrapper that handles NOAA requests with simple retry/backoff.

    Retries on 5xx and 429 responses. Raises for other client errors.
    """
    url = f"{BASE_URL}/{endpoint}"
    max_attempts = 5
    backoff = 2.0

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)

            # successful
            if response.status_code == 200:
                return response.json()

            # for any non-200, print a snippet to aid debugging
            try:
                snippet = response.text[:1000]
                print(f"NOAA API returned {response.status_code} for {url} (attempt {attempt}/{max_attempts}). Response snippet:\n{snippet}")
            except Exception:
                print(f"NOAA API returned {response.status_code} for {url} (attempt {attempt}/{max_attempts})")

            # retryable server-side or rate limit errors
            if response.status_code in (429, 500, 502, 503, 504):
                if attempt < max_attempts:
                    print(f"  ⚠ Rate limit or server error, waiting {backoff} seconds...")
                    time.sleep(backoff + ERROR_BACKOFF_EXTRA)
                    backoff *= 2
                    continue
                else:
                    response.raise_for_status()

            # other client errors - raise immediately
            response.raise_for_status()

        except requests.exceptions.RequestException as exc:
            print(f"  ⚠ Request error on attempt {attempt}/{max_attempts}: {exc}")
            if attempt < max_attempts:
                print(f"  Waiting {backoff + ERROR_BACKOFF_EXTRA} seconds before retry...")
                time.sleep(backoff + ERROR_BACKOFF_EXTRA)
                backoff *= 2
                continue
            raise

    # if we somehow exit the loop without returning, raise a generic error
    raise RuntimeError(f"Failed to GET {url} after {max_attempts} attempts")

def read_station_list():
    """Read station information from airport-list.txt"""
    df = pd.read_csv(STATIONS_FILE)
    stations = []
    
    for _, row in df.iterrows():
        stations.append({
            "id": row["id"],
            "name": row["name"],
            "mindate": row["mindate"],
            "maxdate": row["maxdate"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "elevation": row["elevation"]
        })
    
    return stations

def fetch_station_season(station_id, startdate, enddate):
    """Fetch data for a station between two dates."""
    records = []
    offset = 1

    while True:
        params = {
            "datasetid": DATASET_ID,
            "stationid": station_id,
            "datatypeid": ",".join(DATATYPES),
            "startdate": startdate,
            "enddate": enddate,
            "limit": MAX_LIMIT,
            "offset": offset,
            "units": "standard"  # get data in standard units
        }

        data = noaa_get("data", params)
        results = data.get("results", [])

        if not results:
            break

        records.extend(results)
        if offset > 1: 
            print(f"    → {len(records)} records fetched so far...")
        offset += MAX_LIMIT
        time.sleep(REQUEST_SLEEP)

    return records

def main():
    if not NOAA_TOKEN:
        print("ERROR: NOAA_TOKEN is not set or looks invalid.\nSet a valid NOAA CDO token in the NOAA_TOKEN environment variable.")
        return

    # create output directory
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    
    print(f"Reading stations from {STATIONS_FILE}...")
    stations = read_station_list()
    print(f"Found {len(stations)} stations to process\n")
    print(f"⏱️  Using {REQUEST_SLEEP}s delay between requests + {ERROR_BACKOFF_EXTRA}s after errors")
    print("This will take several hours. The script can be safely stopped and restarted.\n")

    for idx, station in enumerate(stations, 1):
        station_id = station["id"]
        station_name = station["name"]
        
        # create safe filename from station ID
        safe_name = station_id.replace(":", "_")
        output_file = os.path.join(OUTPUT_DIR, f"{safe_name}.csv")
        
        # check if already processed
        if os.path.exists(output_file):
            print(f"[{idx}/{len(stations)}] SKIPPING {station_id} - already exists")
            continue

        start_year = int(station["mindate"][:4])
        end_year = int(station["maxdate"][:4])

        print(f"[{idx}/{len(stations)}] Processing: {station_id}")
        print(f"  Name: {station_name}")
        print(f"  Years: {start_year}–{end_year} ({end_year - start_year + 1} years)")

        all_rows = []
        
        try:
            # process in chunks to show progress less frequently
            total_years = end_year - start_year + 1
            for year in range(start_year, end_year + 1):
                years_done = year - start_year
                if years_done % 10 == 0 or year == start_year:
                    print(f"  Progress: {years_done}/{total_years} years ({year})")
                
                # Jan–Mar
                jan_mar = fetch_station_season(
                    station_id,
                    f"{year}-01-01",
                    f"{year}-03-31"
                )

                # Oct–Dec
                oct_dec = fetch_station_season(
                    station_id,
                    f"{year}-10-01",
                    f"{year}-12-31"
                )

                for record in jan_mar + oct_dec:
                    all_rows.append({
                        "station_id": station_id,
                        "station_name": station_name,
                        "date": record["date"],
                        "datatype": record["datatype"],
                        "value": record["value"],
                        "attributes": record.get("attributes", "")
                    })

            # save station data
            if all_rows:
                df = pd.DataFrame(all_rows)
                df.to_csv(output_file, index=False)
                print(f"  ✓ Saved {len(all_rows)} records to {output_file}")
            else:
                print(f"  ⚠ No data found for {station_id}")
                
        except Exception as e:
            print(f"✗ ERROR processing {station_id}: {e}")
            print("Continuing with next station...")
            continue
        
        print()

    print("\n=== ALL DONE ===")
    print(f"Weather data saved to {OUTPUT_DIR}/")

if __name__ == "__main__":
    main()
