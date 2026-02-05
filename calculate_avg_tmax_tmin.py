from pathlib import Path
import csv
from collections import defaultdict
from datetime import datetime, date
import statistics
import calendar
from wsgiref import headers


SPLIT_DIR = Path('split')
TMIN_DIR = SPLIT_DIR / 'tmin'
TMAX_DIR = SPLIT_DIR / 'tmax'
OUT_FILE = Path('monthly_avgs.csv')
RECENT_CUTOFF = date(2025, 10, 1)
OLD_CUTOFF = date(2000, 10, 1)

# period labels and ordering
PERIODS = ['all', '<2000', '2026']

# set winter months and order
WINTER_MONTHS = [10, 11, 12, 1, 2, 3]
MONTH_ORDER = {m: i for i, m in enumerate(WINTER_MONTHS)}

AVG_OUT_DIR = SPLIT_DIR / 'avg-temp'

AVG_MONTHLY_DIR = Path('average-temp-data') / 'monthly'
AVG_YEARLY_DIR = Path('average-temp-data') / 'yearly'
# 
INCLUDED_MONTHS = [11, 12, 1, 2]
SEASON_ORDER = {11: 0, 12: 1, 1: 2, 2: 3}



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
	"""Yield tuples (station_id, station_display, year, month, value) from a TMIN/TMAX CSV file."""
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
			yield station_id, station_name, d.year, d.month, val


def gather():
	tmin = defaultdict(list)
	tmax = defaultdict(list)
	station_display_cache = {}

	tmin_monthly = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
	tmax_monthly = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
	station_years = defaultdict(set)

	# normalize station name
	station_display_by_id = {}

	def add_record_for_periods(station_name, d, val, store):
		disp = station_display_cache.get(station_name)
		if not disp:
			disp = normalize_display_name(station_name)
			station_display_cache[station_name] = disp
		m = d.month
		# most recent winter
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

	# process temp min files
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
					add_record_for_periods(station_name, d, valf, tmin)
					tmin_monthly[station_id][d.year][d.month].append(valf)
					station_years[station_id].add(d.year)
					# store normalized display name for this station id
					station_display_by_id[station_id] = station_display_cache.get(station_name) or normalize_display_name(station_name)

	# process temp max files
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
					add_record_for_periods(station_name, d, valf, tmax)
					tmax_monthly[station_id][d.year][d.month].append(valf)
					station_years[station_id].add(d.year)
					station_display_by_id[station_id] = station_display_cache.get(station_name) or normalize_display_name(station_name)

	rows = []
	keys = set(list(tmin.keys()) + list(tmax.keys()))
	for (disp, m, period) in keys:
		if m not in WINTER_MONTHS:
			continue
		min_vals = tmin.get((disp, m, period), [])
		max_vals = tmax.get((disp, m, period), [])
		avg_min = round(statistics.mean(min_vals)) if min_vals else ''
		avg_max = round(statistics.mean(max_vals)) if max_vals else ''
		rows.append((m, disp, period, avg_min, avg_max))

	# sort
	period_index = {p: i for i, p in enumerate(PERIODS)}
	rows.sort(key=lambda r: (MONTH_ORDER.get(r[0], 99), r[1].lower(), period_index.get(r[2], 99)))

	# compute monthly averages per station
	monthly_avgs = defaultdict(dict)

	for station_id in set(list(tmin_monthly.keys()) + list(tmax_monthly.keys())):
		ym_keys = set()
		for y in tmin_monthly.get(station_id, {}):
			for m in tmin_monthly[station_id][y]:
				ym_keys.add((y, m))
		for y in tmax_monthly.get(station_id, {}):
			for m in tmax_monthly[station_id][y]:
				ym_keys.add((y, m))
		for (y, m) in ym_keys:
			min_vals = tmin_monthly.get(station_id, {}).get(y, {}).get(m, [])
			max_vals = tmax_monthly.get(station_id, {}).get(y, {}).get(m, [])
			avg_min = round(statistics.mean(min_vals)) if min_vals else None
			avg_max = round(statistics.mean(max_vals)) if max_vals else None
			monthly_avgs[station_id][(y, m)] = (avg_max, avg_min)

	# monthly CSVs
	AVG_MONTHLY_DIR.mkdir(parents=True, exist_ok=True)
	for station_id, ym_map in monthly_avgs.items():
		years = station_years.get(station_id, set())
		if not years:
			continue
		earliest_calendar_year = min(years)
		out_rows = []
		for (y, m), (avg_max, avg_min) in ym_map.items():
			if m not in INCLUDED_MONTHS:
				continue
			season_year = y if m in (11, 12) else (y - 1)
			if season_year < earliest_calendar_year:
				# exclude Jan-Feb-Mar from first year since no corresponding months from year prior.
				continue
			out_rows.append((season_year, SEASON_ORDER.get(m, 99), y, m, avg_max, avg_min))
		out_rows.sort(key=lambda r: (r[0], r[1]))
		disp = station_display_by_id.get(station_id, station_id)
		# normalize filename
		fname = disp.replace(' ', '') + '.csv'
		outpath = AVG_MONTHLY_DIR / fname
		with outpath.open('w', newline='') as fh:
			writer = csv.writer(fh)
			writer.writerow(['year', 'month', 'station', 'avgMaxTemp', 'avgMinTemp'])
			for season_year, _, y, m, avg_max, avg_min in out_rows:
				if avg_max is None and avg_min is None:
					continue
				month_abbr = calendar.month_abbr[m]
				writer.writerow([y, month_abbr, disp, avg_max if avg_max is not None else '', avg_min if avg_min is not None else ''])

	# yearly csv's with Nov-Jan avg
	AVG_YEARLY_DIR.mkdir(parents=True, exist_ok=True)
	for station_id, ym_map in monthly_avgs.items():
		years = station_years.get(station_id, set())
		if not years:
			continue
		earliest_calendar_year = min(years)
		season_years = set()
		for (y, m) in ym_map.keys():
			if m in (11, 12):
				season_years.add(y)
			elif m == 1:
				season_years.add(y - 1)
		season_years = sorted([sy for sy in season_years if sy >= earliest_calendar_year])
		disp = station_display_by_id.get(station_id, station_id)
		fname = disp.replace(' ', '') + '.csv'
		outpath = AVG_YEARLY_DIR / fname
		with outpath.open('w', newline='') as fh:
			writer = csv.writer(fh)
			writer.writerow(['year', 'station', 'avgMaxTemp', 'avgMinTemp'])
			for sy in season_years:
				parts_max = []
				parts_min = []
				v = ym_map.get((sy, 11))
				if v:
					if v[0] is not None:
						parts_max.append(v[0])
					if v[1] is not None:
						parts_min.append(v[1])
				# Dec of sy
				v = ym_map.get((sy, 12))
				if v:
					if v[0] is not None:
						parts_max.append(v[0])
					if v[1] is not None:
						parts_min.append(v[1])
				# Jan of sy+1
				v = ym_map.get((sy + 1, 1))
				if v:
					if v[0] is not None:
						parts_max.append(v[0])
					if v[1] is not None:
						parts_min.append(v[1])
				if not parts_max and not parts_min:
					continue
				avg_max = round(statistics.mean(parts_max)) if parts_max else ''
				avg_min = round(statistics.mean(parts_min)) if parts_min else ''
				writer.writerow([sy, disp, avg_max, avg_min])

		AGG_DIR = Path('analysis') / 'aggregated-avg-temp'
		AGG_DIR.mkdir(parents=True, exist_ok=True)

		all_path = AGG_DIR / 'average-temps-all.csv'
		pre2000_path = AGG_DIR / 'average-temps-pre-2000.csv'

		with all_path.open('w', newline='') as f_all, pre2000_path.open('w', newline='') as f_pre:
			w_all = csv.writer(f_all)
			w_pre = csv.writer(f_pre)
			w_all.writerow(['station', 'avgMax', 'avgMin'])
			w_pre.writerow(['station', 'avgMax', 'avgMin'])

			rows_all = []
			rows_pre = []

			for station_id, ym_map in monthly_avgs.items():
				disp = station_display_by_id.get(station_id, station_id)
				season_years = set()
				for (y, m) in ym_map.keys():
					if m in (11, 12):
						season_years.add(y)
					elif m == 1:
						season_years.add(y - 1)

				# through 2025 average
				parts_max_all = []
				parts_min_all = []
				for sy in season_years:
					if sy >= 2025:
						continue

					v = ym_map.get((sy, 11))
					if v:
						if v[0] is not None:
							parts_max_all.append(v[0])
						if v[1] is not None:
							parts_min_all.append(v[1])

					v = ym_map.get((sy, 12))
					if v:
						if v[0] is not None:
							parts_max_all.append(v[0])
						if v[1] is not None:
							parts_min_all.append(v[1])

					v = ym_map.get((sy + 1, 1))
					if v:
						if v[0] is not None:
							parts_max_all.append(v[0])
						if v[1] is not None:
							parts_min_all.append(v[1])

				avg_max_all = round(statistics.mean(parts_max_all)) if parts_max_all else ''
				avg_min_all = round(statistics.mean(parts_min_all)) if parts_min_all else ''
				rows_all.append((disp, avg_max_all, avg_min_all))

				# through 2000 average
				parts_max_pre = []
				parts_min_pre = []
				for sy in season_years:
					if sy >= 2000:
						continue
					v = ym_map.get((sy, 11))
					if v:
						if v[0] is not None:
							parts_max_pre.append(v[0])
						if v[1] is not None:
							parts_min_pre.append(v[1])
					v = ym_map.get((sy, 12))
					if v:
						if v[0] is not None:
							parts_max_pre.append(v[0])
						if v[1] is not None:
							parts_min_pre.append(v[1])
					v = ym_map.get((sy + 1, 1))
					if v:
						if v[0] is not None:
							parts_max_pre.append(v[0])
						if v[1] is not None:
							parts_min_pre.append(v[1])

				avg_max_pre = round(statistics.mean(parts_max_pre)) if parts_max_pre else ''
				avg_min_pre = round(statistics.mean(parts_min_pre)) if parts_min_pre else ''
				rows_pre.append((disp, avg_max_pre, avg_min_pre))

			# sort by station
			rows_all.sort(key=lambda r: r[0].lower())
			rows_pre.sort(key=lambda r: r[0].lower())
			for r in rows_all:
				w_all.writerow(r)
			for r in rows_pre:
				w_pre.writerow(r)

		return rows


def write_output(rows):
	AVG_OUT_DIR.mkdir(parents=True, exist_ok=True)

	with OUT_FILE.open('w', newline='') as fh:
		writer = csv.writer(fh)
		writer.writerow(['station', 'month', 'year', 'avgMin', 'avgMax'])
		for m, disp, period, avg_min, avg_max in rows:
			month_abbr = calendar.month_abbr[m]
			writer.writerow([disp, month_abbr, period, avg_min, avg_max])

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
	print('Wrote {OUT_FILE} with {len(rows)} rows')


if __name__ == '__main__':
	main()

