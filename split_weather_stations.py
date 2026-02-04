from pathlib import Path
import csv

DATA_DIR = Path("weather-data")
OUT_DIR = Path("split")
TYPES = {"SNOW", "SNWD", "TMAX", "TMIN", "PRCP"}


def normalize_station_name(name: str) -> str:
	return name.strip().lower().replace(' ', '-')


def ensure_dirs():
	OUT_DIR.mkdir(exist_ok=True)
	for t in TYPES:
		(OUT_DIR / t.lower()).mkdir(parents=True, exist_ok=True)


def process_files():
	ensure_dirs()
	for csv_path in DATA_DIR.glob('*.csv'):
		with csv_path.open(newline='') as fh:
			reader = csv.reader(fh)
			for row in reader:
				if not row:
					continue
				if len(row) < 6:
					continue
				station_id, station_name, date, datatype, value, attributes = row[:6]
				if datatype not in TYPES:
					continue
				norm = normalize_station_name(station_name)
				out_dir = OUT_DIR / datatype.lower()
				out_file = out_dir / f"{norm}.csv"
				write_header = not out_file.exists()
				with out_file.open('a', newline='') as outfh:
					writer = csv.writer(outfh)
					if write_header:
						writer.writerow(['station_id','station_name','date','datatype','value','attributes'])
					writer.writerow([station_id, station_name, date, datatype, value, attributes])


if __name__ == '__main__':
	process_files()
	print('Split complete.')

