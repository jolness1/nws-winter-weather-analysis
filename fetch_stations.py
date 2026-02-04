import os
import time
import random
import requests
import csv
import argparse

BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"

# optional: load env from .env files (install python-dotenv to use)
try:
    from dotenv import load_dotenv
    load_dotenv('.env')
except Exception:
    pass

NOAA_TOKEN = os.getenv("NOAA_TOKEN")


def noaa_get(url, headers, params, max_attempts=5, timeout=15):
    backoff = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()

            # print snippet for debugging
            snippet = (resp.text or "")[:1000]
            print(f"NOAA {resp.status_code} for {url}: {snippet}")

            # respect Retry-After header if provided
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    wait = float(ra)
                except Exception:
                    wait = 1.0
                print(f"Respecting Retry-After: sleeping {wait}s")
                time.sleep(wait + random.uniform(0, 0.5))

            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt < max_attempts:
                    time.sleep(backoff + random.uniform(0, 0.5))
                    backoff *= 2
                    continue
                resp.raise_for_status()

            resp.raise_for_status()

        except requests.exceptions.ReadTimeout as e:
            print(f"Read timeout (attempt {attempt}): {e}")
            if attempt < max_attempts:
                time.sleep(backoff + random.uniform(0, 0.5))
                backoff *= 2
                continue
            raise
        except requests.exceptions.RequestException as e:
            print(f"Request error (attempt {attempt}): {e}")
            if attempt < max_attempts:
                time.sleep(backoff + random.uniform(0, 0.5))
                backoff *= 2
                continue
            raise

    raise RuntimeError(f"Failed GET {url} after {max_attempts} attempts")


def fetch_stations(state_fips="FIPS:30", dataset="GHCND", per_page=1000, max_total=None, token=None):
    if token is None:
        raise ValueError("NOAA token required")

    headers = {"token": token}
    url = f"{BASE_URL}/stations"

    stations = []
    offset = 1

    while True:
        params = {
            "datasetid": dataset,
            "locationid": state_fips,
            "limit": per_page,
            "offset": offset,
        }

        data = noaa_get(url, headers, params)
        results = data.get("results", [])
        if not results:
            break

        for st in results:
            stations.append({
                "id": st.get("id"),
                "name": st.get("name"),
                "mindate": st.get("mindate"),
                "maxdate": st.get("maxdate"),
                "latitude": st.get("latitude"),
                "longitude": st.get("longitude"),
                "elevation": st.get("elevation"),
            })
            if max_total and len(stations) >= max_total:
                return stations

        offset += per_page
        time.sleep(0.5 + random.uniform(0, 0.2))

    return stations


def main():
    p = argparse.ArgumentParser(description="Fetch NOAA stations for a state and save CSV")
    p.add_argument("--fips", default="FIPS:30", help="Location ID (default FIPS:30 for MT)")
    p.add_argument("--dataset", default="GHCND")
    p.add_argument("--per-page", type=int, default=1000)
    p.add_argument("--max", type=int, default=None, help="Max total stations to fetch (for testing)")
    p.add_argument("--out", default="stations_montana.csv")
    args = p.parse_args()

    token = NOAA_TOKEN
    if not token:
        print("ERROR: NOAA_TOKEN is not set. Put your token in a .env file or the NOAA_TOKEN environment variable.")
        return

    print(f"Fetching stations for {args.fips} (dataset={args.dataset})...")
    stations = fetch_stations(state_fips=args.fips, dataset=args.dataset, per_page=args.per_page, max_total=args.max, token=token)
    print(f"Fetched {len(stations)} stations")

    # write CSV
    keys = ["id", "name", "mindate", "maxdate", "latitude", "longitude", "elevation"]
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for s in stations:
            writer.writerow(s)

    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
