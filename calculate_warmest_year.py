import os
import csv
from datetime import datetime
from collections import defaultdict


TARGET_MONTHS = [10, 11, 12, 1, 2, 3]
MONTH_LABELS = {
	1: "Jan",
	2: "Feb",
	3: "Mar",
	4: "Apr",
	5: "May",
	6: "Jun",
	7: "Jul",
	8: "Aug",
	9: "Sep",
	10: "Oct",
	11: "Nov",
	12: "Dec",
}
BASE_DIR = os.path.dirname(__file__)
TMAX_DIR = os.path.join(BASE_DIR, "split", "tmax")
TMIN_DIR = os.path.join(BASE_DIR, "split", "tmin")
OUT_BASE = os.path.join(BASE_DIR, "split", "avg-temp-ranking")
YEAR_TIERS_BASE = os.path.join(BASE_DIR, "split", "year-tiers")
ANALYSIS_BASE = os.path.join(BASE_DIR, "analysis")


def compute_winter_avgs(monthly_avgs):
	winter_avgs = {}
	years = {y for (y, m) in monthly_avgs.keys()}
	candidate_years = set()
	for (y, m) in monthly_avgs.keys():
		if m in (11, 12):
			candidate_years.add(y)
		if m == 1:
			candidate_years.add(y - 1)

	for winter in sorted(candidate_years):
		key_nov = (winter, 11)
		key_dec = (winter, 12)
		key_jan = (winter + 1, 1)
		if key_nov in monthly_avgs and key_dec in monthly_avgs and key_jan in monthly_avgs:
			vals = [monthly_avgs[key_nov], monthly_avgs[key_dec], monthly_avgs[key_jan]]
			winter_avgs[winter] = sum(vals) / len(vals)

	return winter_avgs


def generate_analysis_for_dir(in_dir, is_high=True):
	kind = "high" if is_high else "low"
	series_dir = os.path.join(ANALYSIS_BASE, f"{ 'high' if is_high else 'low' }-temp")
	ensure_dir(series_dir)
	all_rows = []

	for fname in sorted(os.listdir(in_dir)):
		if not fname.lower().endswith('.csv'):
			continue
		in_path = os.path.join(in_dir, fname)
		station_basename = os.path.splitext(fname)[0]
		station_name, avgs = process_station_file(in_path)
		winter_avgs = compute_winter_avgs(avgs)

		series_out = os.path.join(series_dir, f"{station_basename}.csv")
		ensure_dir(os.path.dirname(series_out))
		with open(series_out, 'w', newline='', encoding='utf-8') as sf:
			writer = csv.writer(sf)
			writer.writerow(["year", "station", f"avg{ 'High' if is_high else 'Low' }Temp"])
			for year, avg in sorted(winter_avgs.items(), key=lambda t: t[1], reverse=True):
				writer.writerow([year, station_name, f"{avg:.2f}"])

		if winter_avgs:
			sorted_items = sorted(winter_avgs.items(), key=lambda t: t[1], reverse=True)
			top_val = sorted_items[0][1]
			for idx, (year, avg) in enumerate(sorted_items, start=1):
				degrees = round(top_val - avg, 2)
				all_rows.append((station_name, year, idx, degrees))

	ranking_file = os.path.join(ANALYSIS_BASE, f"warmest-winter-{ 'high' if is_high else 'low' }-ranking.csv")
	ensure_dir(os.path.dirname(ranking_file))
	all_rows.sort(key=lambda r: (r[0].lower(), r[2]))
	with open(ranking_file, 'w', newline='', encoding='utf-8') as rf:
		writer = csv.writer(rf)
		writer.writerow(["year", "station", "ranking", "degreesBelowTop"])
		for sname, year, idx, degrees in all_rows:
			writer.writerow([year, sname, idx, degrees])



def ensure_dir(path):
	os.makedirs(path, exist_ok=True)


def process_station_file(in_path):
	"""Read a station CSV and return station_name and a dict of (year,month)->avg_value."""
	groups = defaultdict(list)
	station_name = None
	with open(in_path, newline="", encoding="utf-8") as f:
		reader = csv.DictReader(f)
		for row in reader:
			if not station_name:
				station_name = row.get("station_name") or row.get("station") or ""
			date_s = row.get("date")
			val_s = row.get("value")
			if not date_s or val_s in (None, ""):
				continue
			try:
				if "T" in date_s:
					dt = datetime.fromisoformat(date_s)
				else:
					dt = datetime.strptime(date_s, "%Y-%m-%d")
			except Exception:
				try:
					parts = date_s.split("T")[0].split("-")
					dt = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
				except Exception:
					continue
			try:
				val = float(val_s)
			except Exception:
				continue
			groups[(dt.year, dt.month)].append(val)

	# calculate averages
	avgs = {}
	for (y, m), vals in groups.items():
		if vals:
			avgs[(y, m)] = sum(vals) / len(vals)
	return station_name or os.path.splitext(os.path.basename(in_path))[0], avgs


def write_ranked_output(avgs, station_name, out_path, value_field_name):
	rows = []
	for month in TARGET_MONTHS:
		items = [(y, m, avg) for (y, m), avg in avgs.items() if m == month]
		items.sort(key=lambda t: t[2], reverse=True)
		top = items[:20]
		for y, m, avg in top:
			rows.append((y, m, station_name, avg))

	ensure_dir(os.path.dirname(out_path))
	with open(out_path, "w", newline="", encoding="utf-8") as f:
		writer = csv.writer(f)
		header = ["year", "month", "stationName", value_field_name]
		writer.writerow(header)
		for y, m, sname, avg in rows:
			month_label = MONTH_LABELS.get(m, str(m))
			writer.writerow([y, month_label, sname, f"{avg:.2f}"])


def write_year_tiers(avgs, station_name, out_path, which):
	check_months = [10, 11, 12, 1]
	target_year_for_month = {10: 2025, 11: 2025, 12: 2025, 1: 2026}

	rows = []
	for m in check_months:
		items = [(y, mm, avg) for (y, mm), avg in avgs.items() if mm == m]
		if not items:
			rows.append((target_year_for_month[m], MONTH_LABELS.get(m, str(m)), station_name, "", ""))
			continue
		items.sort(key=lambda t: t[2], reverse=True)
		top_value = items[0][2]
		target_year = target_year_for_month[m]
		ranking = ""
		degrees = ""
		for idx, (y, mm, avg) in enumerate(items, start=1):
			if y == target_year:
				ranking = idx
				degrees = round(top_value - avg, 2)
				break
		rows.append((target_year, MONTH_LABELS.get(m, str(m)), station_name, ranking, degrees))

	ensure_dir(os.path.dirname(out_path))
	with open(out_path, "w", newline="", encoding="utf-8") as f:
		writer = csv.writer(f)
		writer.writerow(["year", "month", "station", "ranking", "degreesBelowTop"])
		for year, month_label, sname, ranking, degrees in rows:
			writer.writerow([year, month_label, sname, ranking, degrees])


def process_directory(in_dir, out_dir, value_field_name):
	ensure_dir(out_dir)
	for fname in sorted(os.listdir(in_dir)):
		if not fname.lower().endswith(".csv"):
			continue
		in_path = os.path.join(in_dir, fname)
		station_basename = os.path.splitext(fname)[0]
		station_name, avgs = process_station_file(in_path)
		out_path = os.path.join(out_dir, f"{station_basename}.csv")
		write_ranked_output(avgs, station_name, out_path, value_field_name)
		tiers_base = os.path.join(YEAR_TIERS_BASE, "high" if value_field_name == "avgHighTemp" else "low")
		ensure_dir(tiers_base)
		tiers_out_path = os.path.join(tiers_base, f"{station_basename}.csv")
		write_year_tiers(avgs, station_name, tiers_out_path, "high" if value_field_name == "avgHighTemp" else "low")


def main():
	high_out = os.path.join(OUT_BASE, "high")
	low_out = os.path.join(OUT_BASE, "low")

	if os.path.isdir(TMAX_DIR):
		process_directory(TMAX_DIR, high_out, "avgHighTemp")
	else:
		print(f"TMAX directory not found: {TMAX_DIR}")

	if os.path.isdir(TMIN_DIR):
		process_directory(TMIN_DIR, low_out, "avgLowTemp")
	else:
		print(f"TMIN directory not found: {TMIN_DIR}")

	if os.path.isdir(TMAX_DIR):
		generate_analysis_for_dir(TMAX_DIR, is_high=True)
	if os.path.isdir(TMIN_DIR):
		generate_analysis_for_dir(TMIN_DIR, is_high=False)


if __name__ == "__main__":
	main()

