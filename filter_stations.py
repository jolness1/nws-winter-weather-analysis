import csv
import argparse


def filter_and_sort(input_csv, output_csv, year_prefix="2026"):
    rows = []
    with open(input_csv, newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            maxdate = (r.get("maxdate") or "")
            if maxdate.startswith(year_prefix):
                rows.append(r)

    # sort by mindate ascending (empty mindate go to end)
    rows.sort(key=lambda r: r.get("mindate") or "9999-12-31")

    if not rows:
        print(f"No rows with maxdate starting with {year_prefix}")
        return

    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_csv}")
    # print first 10 lines
    for r in rows[:10]:
        print(r["id"], r["name"], r.get("mindate"), r.get("maxdate"))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="input", default="stations_montana.csv")
    p.add_argument("--out", dest="output", default="stations_2026_sorted.csv")
    p.add_argument("--year", dest="year", default="2026")
    args = p.parse_args()

    filter_and_sort(args.input, args.output, args.year)
