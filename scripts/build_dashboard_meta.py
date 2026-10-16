"""Build src/dashboard/meta.json (airlines, airports, routes) from the BTS CSV for the dashboard dropdowns."""
import csv, json, statistics, sys
from collections import defaultdict

AIRLINES = {
    "9E": "Endeavor Air", "AA": "American Airlines", "AS": "Alaska Airlines", "B6": "JetBlue Airways",
    "DL": "Delta Air Lines", "F9": "Frontier Airlines", "G4": "Allegiant Air", "HA": "Hawaiian Airlines",
    "MQ": "Envoy Air", "NK": "Spirit Airlines", "OH": "PSA Airlines", "OO": "SkyWest Airlines",
    "UA": "United Airlines", "WN": "Southwest Airlines", "YX": "Republic Airways",
}
src, out = sys.argv[1], sys.argv[2]
airports, routes, carriers = {}, defaultdict(lambda: {"d": [], "t": [], "c": set()}), set()
origins, dests = set(), set()
for r in csv.DictReader(open(src, newline="")):
    if not r["Distance"] or not r["CRSElapsedTime"]:
        continue
    carriers.add(r["Reporting_Airline"])
    for k, p in (("Origin", "Origin"), ("Dest", "Dest")):
        airports[r[k]] = {"city": r[p + "CityName"], "state": r[p + "State"]}
    origins.add(r["Origin"]); dests.add(r["Dest"])
    rt = routes[f'{r["Origin"]}-{r["Dest"]}']
    rt["d"].append(float(r["Distance"])); rt["t"].append(float(r["CRSElapsedTime"])); rt["c"].add(r["Reporting_Airline"])
meta = {
    "airlines": [{"code": c, "name": AIRLINES.get(c, c)} for c in sorted(carriers)],
    "airports": {k: v for k, v in sorted(airports.items())},
    "origins": sorted(origins), "dests": sorted(dests),
    "routes": {k: {"distance": round(statistics.median(v["d"])), "minutes": round(statistics.median(v["t"])),
                   "airlines": sorted(v["c"])} for k, v in sorted(routes.items())},
}
json.dump(meta, open(out, "w"), separators=(",", ":"))
print(len(meta["airlines"]), "airlines,", len(airports), "airports,", len(routes), "routes")
