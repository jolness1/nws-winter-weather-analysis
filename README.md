# nws-winter-weather-analysis
Evaluating whether the weather (lol) this winter in Montana really is warmer than "normal" and other trends

## Prerequisites
- Python 3 — tested on 3.14

## Getting started

Follow these steps to create a local virtual environment, install dependencies, and create your `.env` file (do not commit your real `.env`):

1. Create and activate the virtual environment, install requirements

```bash
# create a virtual environment named .venv in the project root
python3 -m venv .venv

# activate the venv for this shell (macOS / Linux)
source .venv/bin/activate

# upgrade pip and install required packages
pip install --upgrade pip
pip install -r requirements.txt
```

2. Copy the example env and add your NOAA token

```bash
# copy the example .env to a local .env (.env is in .gitignore)
cp .env.example .env

# edit .env and set NOAA_TOKEN (use your editor of choice)
NOAA_TOKEN=your_noaa_token_here
```

3. Run the scripts

```bash
# fetch stations (example)
python fetch_stations.py

# fetch data for stations listed in airport-list.txt
python get_stations.py
```

_Notes_:
- Keep your real `.env` out of version control. `.env` is already listed in `.gitignore`.
- If you run into rate limits, increase delays and run them overnight or adjust date ranges for faster testing.
