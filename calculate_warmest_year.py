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
				# ISO-like format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
				if "T" in date_s:
					dt = datetime.fromisoformat(date_s)
				else:
					dt = datetime.strptime(date_s, "%Y-%m-%d")
			except Exception:
				# Fallback: split
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

	# compute averages
	avgs = {}
	for (y, m), vals in groups.items():
		if vals:
			avgs[(y, m)] = sum(vals) / len(vals)
	return station_name or os.path.splitext(os.path.basename(in_path))[0], avgs


def write_ranked_output(avgs, station_name, out_path, value_field_name):
	rows = []
	for month in TARGET_MONTHS:
		# gather all (year,month) that match this month
		items = [(y, m, avg) for (y, m), avg in avgs.items() if m == month]
		# sort descending by avg
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
	"""Write a small CSV showing ranking and degreesBelowTop for 2025/2026.

	which: 'high' or 'low' (used only for file placement/logging).
	For Oct/Nov/Dec use year 2025; for Jan use 2026.
	"""
	# months to check: Oct, Nov, Dec, Jan
	check_months = [10, 11, 12, 1]
	target_year_for_month = {10: 2025, 11: 2025, 12: 2025, 1: 2026}

	rows = []
	for m in check_months:
		# gather items for this month
		items = [(y, mm, avg) for (y, mm), avg in avgs.items() if mm == m]
		if not items:
			# no data for this month
			rows.append((target_year_for_month[m], MONTH_LABELS.get(m, str(m)), station_name, "", ""))
			continue
		# sort descending
		items.sort(key=lambda t: t[2], reverse=True)
		top_value = items[0][2]
		target_year = target_year_for_month[m]
		# find the target year entry
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
		# write year-tier output alongside ranked outputs
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


if __name__ == "__main__":
	main()

