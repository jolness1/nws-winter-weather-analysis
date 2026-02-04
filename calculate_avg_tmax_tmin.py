from pathlib import Path
import csv
from collections import defaultdict
from datetime import datetime, date
import statistics
import calendar


SPLIT_DIR = Path('split')
TMIN_DIR = SPLIT_DIR / 'tmin'
TMAX_DIR = SPLIT_DIR / 'tmax'
OUT_FILE = Path('monthly_avgs.csv')
RECENT_CUTOFF = date(2025, 10, 1)
OLD_CUTOFF = date(2000, 10, 1)

# period labels and ordering
PERIODS = ['all', '<2000', '2026']

# winter months and desired order: Oct, Nov, Dec, Jan, Feb, Mar
WINTER_MONTHS = [10, 11, 12, 1, 2, 3]
MONTH_ORDER = {m: i for i, m in enumerate(WINTER_MONTHS)}

AVG_OUT_DIR = SPLIT_DIR / 'avg-temp'



def normalize_display_name(full_name: str) -> str:
	# remove commas and extra spaces
	s = full_name.split(',')[0].strip()
	parts = [p for p in s.replace('-', ' ').split() if p]
	if len(parts) >= 2 and parts[0].lower() == 'miles' and parts[1].lower() == 'city':
		chosen = parts[:2]
	elif len(parts) >= 2 and parts[0].lower() == 'great' and parts[1].lower() == 'falls':
		chosen = parts[:2]
	else:
		chosen = parts[:1]
	return ' '.join(w.title() for w in chosen)


def read_station_file(path: Path):
	"""Yield tuples (station_display, year, month, value) from a TMIN/TMAX CSV file."""
	with path.open(newline='') as fh:
		reader = csv.reader(fh)
		try:
			header = next(reader)
		except StopIteration:
			return
		for row in reader:
			if len(row) < 6:
				continue
			station_id, station_name, dt, datatype, value, attributes = row[:6]
			try:
				# date format: YYYY-MM-DDTHH:MM:SS
				d = datetime.fromisoformat(dt).date()
			except Exception:
				try:
					d = datetime.strptime(dt.split('T')[0], '%Y-%m-%d').date()
				except Exception:
					continue
			try:
				val = float(value)
			except Exception:
				continue
			yield station_name, d.year, d.month, val


def gather():
	"""Gather per-station, per-month averages for three periods:
	- 'all' : all dates before RECENT_CUTOFF
	- '<2000': dates before OLD_CUTOFF
	- '2026': dates on/after RECENT_CUTOFF
	"""
	# keys: (station_display, month, period_label) -> list of vals
	tmin = defaultdict(list)
	tmax = defaultdict(list)
	station_display_cache = {}

	def add_record(station_name, d, val, store):
		disp = station_display_cache.get(station_name)
		if not disp:
			disp = normalize_display_name(station_name)
			station_display_cache[station_name] = disp
		m = d.month
		# recent winter bucket
		if d >= RECENT_CUTOFF:
			key = (disp, m, '2026')
			store[key].append(val)
			return
		# otherwise it's part of 'all'
		key_all = (disp, m, 'all')
		store[key_all].append(val)
		# and also may be part of '<2000'
		if d < OLD_CUTOFF:
			key_old = (disp, m, '<2000')
			store[key_old].append(val)

	# process tmin files
	if TMIN_DIR.exists():
		for p in TMIN_DIR.glob('*.csv'):
			for station_name, y, m, val in read_station_file(p):
				# reconstruct date from y,m with day irrelevant; read_station_file returned year/month
				# but we need full date to compare to cutoffs; instead, read original file to get full date
				# so re-open and parse lines directly here to get exact date strings
				pass

	# to get accurate dates we must re-read files with full date parsing
	# process tmin files
	if TMIN_DIR.exists():
		for p in TMIN_DIR.glob('*.csv'):
			with p.open(newline='') as fh:
				reader = csv.reader(fh)
				try:
					next(reader)
				except StopIteration:
					continue
				for row in reader:
					if len(row) < 6:
						continue
					station_id, station_name, dt, datatype, value, attributes = row[:6]
					if datatype != 'TMIN':
						continue
					try:
						d = datetime.fromisoformat(dt).date()
					except Exception:
						try:
							d = datetime.strptime(dt.split('T')[0], '%Y-%m-%d').date()
						except Exception:
							continue
					try:
						valf = float(value)
					except Exception:
						continue
					add_record(station_name, d, valf, tmin)

	# process tmax files
	if TMAX_DIR.exists():
		for p in TMAX_DIR.glob('*.csv'):
			with p.open(newline='') as fh:
				reader = csv.reader(fh)
				try:
					next(reader)
				except StopIteration:
					continue
				for row in reader:
					if len(row) < 6:
						continue
					station_id, station_name, dt, datatype, value, attributes = row[:6]
					if datatype != 'TMAX':
						continue
					try:
						d = datetime.fromisoformat(dt).date()
					except Exception:
						try:
							d = datetime.strptime(dt.split('T')[0], '%Y-%m-%d').date()
						except Exception:
							continue
					try:
						valf = float(value)
					except Exception:
						continue
					add_record(station_name, d, valf, tmax)

	# build rows
	rows = []
	# union of keys across tmin and tmax
	keys = set(list(tmin.keys()) + list(tmax.keys()))
	for (disp, m, period) in keys:
		# only include winter months
		if m not in WINTER_MONTHS:
			continue
		min_vals = tmin.get((disp, m, period), [])
		max_vals = tmax.get((disp, m, period), [])
		avg_min = round(statistics.mean(min_vals)) if min_vals else ''
		avg_max = round(statistics.mean(max_vals)) if max_vals else ''
		rows.append((m, disp, period, avg_min, avg_max))

	# sort by our winter month order, then station name, then period order
	period_index = {p: i for i, p in enumerate(PERIODS)}
	rows.sort(key=lambda r: (MONTH_ORDER.get(r[0], 99), r[1].lower(), period_index.get(r[2], 99)))
	return rows


def write_output(rows):
	# ensure per-station output dir exists
	AVG_OUT_DIR.mkdir(parents=True, exist_ok=True)

	# write the aggregated CSV
	with OUT_FILE.open('w', newline='') as fh:
		writer = csv.writer(fh)
		writer.writerow(['station', 'month', 'year', 'avgMin', 'avgMax'])
		for m, disp, period, avg_min, avg_max in rows:
			month_abbr = calendar.month_abbr[m]
			writer.writerow([disp, month_abbr, period, avg_min, avg_max])

	# also write one CSV per station
	per_station = {}
	for m, disp, period, avg_min, avg_max in rows:
		per_station.setdefault(disp, []).append((m, period, avg_min, avg_max))

	for disp, entries in per_station.items():
		fname = disp.lower().replace(' ', '-') + '.csv'
		outpath = AVG_OUT_DIR / fname
		with outpath.open('w', newline='') as fh:
			writer = csv.writer(fh)
			writer.writerow(['station', 'month', 'year', 'avgMin', 'avgMax'])
			for m, period, avg_min, avg_max in entries:
				month_abbr = calendar.month_abbr[m]
				writer.writerow([disp, month_abbr, period, avg_min, avg_max])


def main():
	rows = gather()
	write_output(rows)
	print(f'Wrote {OUT_FILE} with {len(rows)} rows')


if __name__ == '__main__':
	main()

