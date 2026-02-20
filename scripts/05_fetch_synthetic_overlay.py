#!/usr/bin/env python3
"""
Fetch source-5 proxy datasets and build synthetic operational overlay tables.

Outputs a timestamped folder under data/j-synthetic_ops_overlay by default.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_ROOT = ROOT / "data" / "j-synthetic_ops_overlay"

BTS_BAGGAGE_CSV = "https://data.bts.gov/resource/6u8d-47ih.csv?$limit=5000"
BTS_BAGGAGE_META = "https://data.bts.gov/api/views/6u8d-47ih"
BTS_ONTIME_META = "https://data.bts.gov/api/views/56fa-sf82"
BTS_ONTIME_ACCESS_TEST = "https://data.bts.gov/resource/56fa-sf82.csv?$limit=5"

SDR_QUERY_URL = "https://sdrs.faa.gov/Query.aspx"
CODA_PAGE = "https://www.eurocontrol.int/publication/all-causes-delays-air-transport-europe-annual-2024"


def run(cmd: List[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def curl_download(url: str, dst: Path, extra: Optional[List[str]] = None) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["curl", "-sSL", url, "-o", str(dst)]
    if extra:
        cmd[1:1] = extra
    run(cmd)


def latest_file(glob_pattern: str) -> Path:
    files = sorted(ROOT.glob(glob_pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No files match {glob_pattern}")
    return files[0]


def parse_hidden_field(html: str, name: str) -> str:
    m = re.search(rf'name="{name}" id="{name}" value="([^"]*)"', html)
    return m.group(1) if m else ""


class SDRResultTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_target_table = False
        self.in_row = False
        self.in_cell = False
        self.rows: List[List[str]] = []
        self.row: List[str] = []
        self.cell: List[str] = []
        self.table_id = "ctl00_pageContentPlaceHolder_dgQueryResults"

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        d = dict(attrs)
        if tag == "table" and d.get("id") == self.table_id:
            self.in_target_table = True
        if not self.in_target_table:
            return
        if tag == "tr":
            self.in_row = True
            self.row = []
        elif self.in_row and tag == "td":
            self.in_cell = True
            self.cell = []

    def handle_endtag(self, tag: str) -> None:
        if self.in_target_table and tag == "table":
            self.in_target_table = False
        if not self.in_target_table:
            return
        if self.in_row and tag == "td":
            value = " ".join("".join(self.cell).strip().split())
            self.row.append(value)
            self.in_cell = False
        elif self.in_row and tag == "tr":
            if self.row:
                self.rows.append(self.row)
            self.in_row = False

    def handle_data(self, data: str) -> None:
        if self.in_target_table and self.in_row and self.in_cell:
            self.cell.append(data)


def run_sdr_query(out_raw: Path, out_csv: Path, *, date_from: str, date_to: str) -> int:
    cookie = out_raw.parent / "sdr_cookie.txt"
    query_get = out_raw.parent / "sdr_query_get.html"

    run(["curl", "-s", "-c", str(cookie), SDR_QUERY_URL, "-o", str(query_get)])
    html_get = query_get.read_text(encoding="utf-8", errors="ignore")

    viewstate = parse_hidden_field(html_get, "__VIEWSTATE")
    viewstategen = parse_hidden_field(html_get, "__VIEWSTATEGENERATOR")
    eventvalidation = parse_hidden_field(html_get, "__EVENTVALIDATION")

    cmd = [
        "curl",
        "-s",
        "-b",
        str(cookie),
        "-c",
        str(cookie),
        SDR_QUERY_URL,
        "--data-urlencode",
        f"__VIEWSTATE={viewstate}",
        "--data-urlencode",
        f"__VIEWSTATEGENERATOR={viewstategen}",
        "--data-urlencode",
        f"__EVENTVALIDATION={eventvalidation}",
        "--data-urlencode",
        "__EVENTTARGET=",
        "--data-urlencode",
        "__EVENTARGUMENT=",
        "--data-urlencode",
        f"ctl00$pageContentPlaceHolder$tbDifficultyDateFrom={date_from}",
        "--data-urlencode",
        f"ctl00$pageContentPlaceHolder$tbDifficultyDateTo={date_to}",
        "--data-urlencode",
        "ctl00$pageContentPlaceHolder$btnQuery=Run Query",
        "-o",
        str(out_raw),
    ]
    run(cmd)

    html_post = out_raw.read_text(encoding="utf-8", errors="ignore")
    p = SDRResultTableParser()
    p.feed(html_post)
    rows = [r for r in p.rows if len(r) == 8]
    data_rows = rows[1:] if rows else []

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "selected",
                "unique_control_no",
                "operator_designator",
                "difficulty_date",
                "n_number",
                "aircraft_make",
                "aircraft_model",
                "jasc_code",
            ]
        )
        w.writerows(data_rows)
    return len(data_rows)


@dataclass
class Airport:
    iata: str
    icao: str
    name: str
    lat: float
    lon: float


def load_openflights_airports(path: Path) -> Tuple[Dict[str, Airport], Dict[str, Airport]]:
    iata_map: Dict[str, Airport] = {}
    icao_map: Dict[str, Airport] = {}
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 8:
                continue
            iata = row[4].strip()
            icao = row[5].strip()
            if iata == r"\N":
                iata = ""
            if icao == r"\N":
                icao = ""
            try:
                lat = float(row[6])
                lon = float(row[7])
            except ValueError:
                continue
            ap = Airport(iata=iata, icao=icao, name=row[1].strip(), lat=lat, lon=lon)
            if iata:
                iata_map[iata] = ap
            if icao:
                icao_map[icao] = ap
    return iata_map, icao_map


def load_openflights_routes(path: Path) -> List[Tuple[str, str, str]]:
    routes: List[Tuple[str, str, str]] = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 6:
                continue
            carrier = row[0].strip()
            src = row[2].strip()
            dst = row[4].strip()
            if not carrier or src in ("", r"\N") or dst in ("", r"\N"):
                continue
            routes.append((carrier, src, dst))
    return routes


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r_km = 6371.0
    p = math.pi / 180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    km = r_km * c
    return km * 0.539957


def load_opensky_departures(paths: Iterable[Path]) -> List[dict]:
    out: List[dict] = []
    for p in paths:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, list):
                out.extend([x for x in data if isinstance(x, dict)])
        except json.JSONDecodeError:
            continue
    return out


def read_baggage_rate(path: Path) -> float:
    total_pax = 0.0
    total_mis = 0.0
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                pax = float((row.get("passengers") or "").strip() or 0)
                mis = float((row.get("mishandled_baggage") or "").strip() or 0)
            except ValueError:
                continue
            total_pax += pax
            total_mis += mis
    if total_pax <= 0:
        return 0.003
    return max(0.0005, min(0.03, total_mis / total_pax))


def read_sdr_jasc_distribution(path: Path) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("jasc_code") or "").strip()
            if not code:
                continue
            counts[code] = counts.get(code, 0) + 1
    if not counts:
        counts = {"5210": 10, "5310": 8, "5320": 6}
    return counts


def pick_weighted(items: List[Tuple[str, float]], rnd: random.Random) -> str:
    total = sum(w for _, w in items)
    x = rnd.random() * total
    c = 0.0
    for k, w in items:
        c += w
        if x <= c:
            return k
    return items[-1][0]


def synthesize_tables(
    out_dir: Path,
    routes: List[Tuple[str, str, str]],
    iata_airports: Dict[str, Airport],
    icao_airports: Dict[str, Airport],
    opensky_departures: List[dict],
    baggage_rate: float,
    sdr_jasc_counts: Dict[str, int],
) -> Dict[str, int]:
    rnd = random.Random(20260219)
    synth_dir = out_dir / "synthetic"
    synth_dir.mkdir(parents=True, exist_ok=True)

    ist_set = {"IST", "SAW", "ISL"}
    ist_routes = [r for r in routes if r[1] in ist_set or r[2] in ist_set]
    if not ist_routes:
        ist_routes = routes[:200]

    base = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    legs: List[dict] = []

    for i in range(min(120, len(ist_routes))):
        carrier, src, dst = ist_routes[i % len(ist_routes)]
        day_offset = i // 20
        dep = base + timedelta(days=day_offset, hours=(i * 3) % 24, minutes=(i * 7) % 60)
        if src in iata_airports and dst in iata_airports:
            dist_nm = haversine_nm(
                iata_airports[src].lat,
                iata_airports[src].lon,
                iata_airports[dst].lat,
                iata_airports[dst].lon,
            )
            block_min = max(50, min(360, int(dist_nm / 7.4)))
        else:
            dist_nm = float(300 + (i * 17) % 1100)
            block_min = int(70 + (i * 11) % 220)
        legs.append(
            {
                "leg_id": f"LEG{i+1:04d}",
                "source": "openflights_route",
                "carrier_code": carrier,
                "flight_no": f"{carrier}{1000 + i}",
                "origin_iata": src,
                "dest_iata": dst,
                "scheduled_dep_utc": dep.isoformat(),
                "scheduled_arr_utc": (dep + timedelta(minutes=block_min)).isoformat(),
                "tailnum": f"N{5200 + i}TX",
                "distance_nm": round(dist_nm, 1),
                "passengers": 80 + (i * 9) % 240,
            }
        )

    for i, dep_evt in enumerate(opensky_departures[:60], start=1):
        origin_icao = (dep_evt.get("estDepartureAirport") or "").strip()
        ap = icao_airports.get(origin_icao)
        origin_iata = ap.iata if ap and ap.iata else "UNK"
        dest_iata = "IST" if origin_iata != "IST" else "SAW"
        ts = dep_evt.get("firstSeen")
        if isinstance(ts, int):
            dep_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            dep_dt = base + timedelta(hours=i)
        block = 80 + (i * 5) % 180
        callsign = (dep_evt.get("callsign") or "").strip().replace(" ", "")
        legs.append(
            {
                "leg_id": f"OSL{i:04d}",
                "source": "opensky_live",
                "carrier_code": callsign[:3] if len(callsign) >= 3 else "UNK",
                "flight_no": callsign or f"OS{i:04d}",
                "origin_iata": origin_iata,
                "dest_iata": dest_iata,
                "scheduled_dep_utc": dep_dt.isoformat(),
                "scheduled_arr_utc": (dep_dt + timedelta(minutes=block)).isoformat(),
                "tailnum": (dep_evt.get("icao24") or f"os{i:04d}").upper(),
                "distance_nm": float(200 + (i * 31) % 900),
                "passengers": 90 + (i * 7) % 210,
            }
        )

    legs.sort(key=lambda x: x["scheduled_dep_utc"])

    cause_weights = [
        ("NONE", 0.36),
        ("WX", 0.15),
        ("ATC", 0.16),
        ("MX", 0.12),
        ("BAG", 0.09),
        ("CREW", 0.08),
        ("SEC", 0.04),
    ]

    milestones: List[dict] = []
    bag_events: List[dict] = []
    crew_rows: List[dict] = []
    tech_rows: List[dict] = []
    graph_edges: List[dict] = []

    jasc_codes = list(sdr_jasc_counts.keys())
    jasc_w = [sdr_jasc_counts[k] for k in jasc_codes]
    jasc_total = sum(jasc_w)

    crew_duty_hours: Dict[str, float] = {}
    captain_pool = [f"CAP{i:03d}" for i in range(1, 41)]
    fo_pool = [f"FO{i:03d}" for i in range(1, 41)]
    cabin_pool = [f"CAB{i:03d}" for i in range(1, 81)]

    for i, leg in enumerate(legs, start=1):
        dep = datetime.fromisoformat(leg["scheduled_dep_utc"])
        arr = datetime.fromisoformat(leg["scheduled_arr_utc"])
        cause = pick_weighted(cause_weights, rnd)

        if cause == "NONE":
            dep_delay = rnd.randint(-5, 10)
        elif cause == "WX":
            dep_delay = rnd.randint(18, 70)
        elif cause == "ATC":
            dep_delay = rnd.randint(8, 45)
        elif cause == "MX":
            dep_delay = rnd.randint(20, 120)
        elif cause == "BAG":
            dep_delay = rnd.randint(10, 40)
        elif cause == "CREW":
            dep_delay = rnd.randint(12, 65)
        else:
            dep_delay = rnd.randint(8, 30)

        event_points = [
            ("GATE_OPEN", -55),
            ("BOARDING_START", -45),
            ("FUELING_START", -40),
            ("FUELING_END", -22),
            ("CATERING_DONE", -18),
            ("BAGGAGE_LOAD_DONE", -12),
            ("PUSHBACK", -5),
            ("TAKEOFF", dep_delay),
        ]
        for j, (milestone, offset_min) in enumerate(event_points, start=1):
            t = dep + timedelta(minutes=offset_min)
            milestones.append(
                {
                    "milestone_id": f"MIL{i:04d}-{j:02d}",
                    "leg_id": leg["leg_id"],
                    "milestone": milestone,
                    "event_ts_utc": t.isoformat(),
                    "status": "done",
                    "delay_cause_code": cause,
                }
            )

        bag_count = int(float(leg["passengers"]) * 1.12)
        mishandled = int(round(bag_count * baggage_rate * (0.4 + rnd.random())))
        if cause == "BAG":
            mishandled = max(mishandled, 1 + rnd.randint(0, 2))
        bag_events.extend(
            [
                {
                    "bag_event_id": f"BAG{i:04d}-01",
                    "leg_id": leg["leg_id"],
                    "event_type": "CHECKIN_LOADED",
                    "event_ts_utc": (dep - timedelta(minutes=65)).isoformat(),
                    "bag_count": bag_count,
                    "status": "ok",
                    "root_cause": "",
                },
                {
                    "bag_event_id": f"BAG{i:04d}-02",
                    "leg_id": leg["leg_id"],
                    "event_type": "TRANSFER_SORTED",
                    "event_ts_utc": (dep - timedelta(minutes=35)).isoformat(),
                    "bag_count": int(bag_count * 0.92),
                    "status": "ok",
                    "root_cause": "",
                },
                {
                    "bag_event_id": f"BAG{i:04d}-03",
                    "leg_id": leg["leg_id"],
                    "event_type": "UNLOADED_ARRIVAL",
                    "event_ts_utc": (arr + timedelta(minutes=12)).isoformat(),
                    "bag_count": bag_count - mishandled,
                    "status": "ok",
                    "root_cause": "",
                },
            ]
        )
        if mishandled > 0:
            bag_events.append(
                {
                    "bag_event_id": f"BAG{i:04d}-99",
                    "leg_id": leg["leg_id"],
                    "event_type": "MISHANDLED_TRIAGE",
                    "event_ts_utc": (arr + timedelta(minutes=35)).isoformat(),
                    "bag_count": mishandled,
                    "status": "open",
                    "root_cause": "late_transfer" if cause in {"ATC", "WX"} else "tag_read_error",
                }
            )

        captain = captain_pool[i % len(captain_pool)]
        first_officer = fo_pool[i % len(fo_pool)]
        cabin_lead = cabin_pool[i % len(cabin_pool)]
        role_set = [("captain", captain), ("first_officer", first_officer), ("cabin_lead", cabin_lead)]

        duty_block_h = (arr - dep).total_seconds() / 3600.0 + 1.8
        for role, crew_id in role_set:
            cumulative = crew_duty_hours.get(crew_id, 0.0) + duty_block_h
            crew_duty_hours[crew_id] = cumulative
            crew_rows.append(
                {
                    "duty_id": f"DUTY-{crew_id}-{i:04d}",
                    "crew_id": crew_id,
                    "role": role,
                    "leg_id": leg["leg_id"],
                    "duty_start_utc": (dep - timedelta(minutes=90)).isoformat(),
                    "duty_end_utc": (arr + timedelta(minutes=25)).isoformat(),
                    "cumulative_duty_hours": round(cumulative, 2),
                    "legality_risk_flag": 1 if cumulative > 10.5 else 0,
                }
            )

        if rnd.random() < 0.13 or cause == "MX":
            x = rnd.random() * jasc_total
            c = 0
            jasc = jasc_codes[0]
            for code, w in zip(jasc_codes, jasc_w):
                c += w
                if x <= c:
                    jasc = code
                    break
            tech_rows.append(
                {
                    "tech_event_id": f"TECH{i:04d}",
                    "leg_id": leg["leg_id"],
                    "event_ts_utc": (dep - timedelta(minutes=24)).isoformat(),
                    "jasc_code": jasc,
                    "mel_category": "B" if rnd.random() < 0.7 else "C",
                    "deferred_flag": 1 if rnd.random() < 0.55 else 0,
                    "severity": "major" if cause == "MX" else "minor",
                    "source_proxy": "faa_sdr",
                    "discrepancy_note": f"Derived from SDR JASC {jasc}",
                }
            )

        graph_edges.extend(
            [
                {
                    "src_type": "Airport",
                    "src_id": leg["origin_iata"],
                    "edge_type": "DEPARTS",
                    "dst_type": "FlightLeg",
                    "dst_id": leg["leg_id"],
                },
                {
                    "src_type": "FlightLeg",
                    "src_id": leg["leg_id"],
                    "edge_type": "ARRIVES",
                    "dst_type": "Airport",
                    "dst_id": leg["dest_iata"],
                },
                {
                    "src_type": "Tail",
                    "src_id": leg["tailnum"],
                    "edge_type": "OPERATES",
                    "dst_type": "FlightLeg",
                    "dst_id": leg["leg_id"],
                },
            ]
        )

    def write_csv(path: Path, rows: List[dict]) -> None:
        if not rows:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    write_csv(synth_dir / "ops_flight_legs.csv", legs)
    write_csv(synth_dir / "ops_turnaround_milestones.csv", milestones)
    write_csv(synth_dir / "ops_baggage_events.csv", bag_events)
    write_csv(synth_dir / "ops_crew_rosters.csv", crew_rows)
    write_csv(synth_dir / "ops_mel_techlog_events.csv", tech_rows)
    write_csv(synth_dir / "ops_graph_edges.csv", graph_edges)

    return {
        "flight_legs": len(legs),
        "turnaround_milestones": len(milestones),
        "baggage_events": len(bag_events),
        "crew_rows": len(crew_rows),
        "tech_events": len(tech_rows),
        "graph_edges": len(graph_edges),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and synthesize source-5 synthetic ops overlays")
    parser.add_argument("--out-root", default=str(DEFAULT_OUT_ROOT), help="Root output directory")
    parser.add_argument("--sdr-from", default="01/01/2026", help="SDR query from date mm/dd/yyyy")
    parser.add_argument("--sdr-to", default="01/03/2026", help="SDR query to date mm/dd/yyyy")
    args = parser.parse_args()

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.out_root) / ts
    raw_dir = out_dir / "raw"
    synth_dir = out_dir / "synthetic"
    raw_dir.mkdir(parents=True, exist_ok=True)
    synth_dir.mkdir(parents=True, exist_ok=True)

    # Proxy downloads.
    baggage_csv = raw_dir / "bts_mishandled_baggage_6u8d-47ih.csv"
    baggage_meta = raw_dir / "bts_mishandled_baggage_6u8d-47ih_metadata.json"
    ontime_meta = raw_dir / "bts_on_time_measure_56fa-sf82_metadata.json"
    ontime_access = raw_dir / "bts_on_time_measure_56fa-sf82_access_test.csv"
    coda_page = raw_dir / "eurocontrol_coda_annual_2024.html"
    coda_pdf = raw_dir / "eurocontrol_coda_annual_2024.pdf"

    curl_download(BTS_BAGGAGE_CSV, baggage_csv)
    curl_download(BTS_BAGGAGE_META, baggage_meta)
    curl_download(BTS_ONTIME_META, ontime_meta)
    curl_download(BTS_ONTIME_ACCESS_TEST, ontime_access)
    curl_download(CODA_PAGE, coda_page)

    coda_html = coda_page.read_text(encoding="utf-8", errors="ignore")
    m_pdf = re.search(r'href="(/sites/default/files/[^"]+coda-digest-annual-report-2024\.pdf)"', coda_html)
    if m_pdf:
        coda_pdf_url = "https://www.eurocontrol.int" + m_pdf.group(1)
        curl_download(coda_pdf_url, coda_pdf)

    # FAA SDR extraction.
    sdr_html = raw_dir / "faa_sdr_query_2026_3day.html"
    sdr_csv = raw_dir / "faa_sdr_query_2026_3day_extract.csv"
    sdr_rows = run_sdr_query(sdr_html, sdr_csv, date_from=args.sdr_from, date_to=args.sdr_to)

    # References.
    asrs_ref = raw_dir / "asrs_reference.txt"
    asrs_ref.write_text(
        "ASRS proxy source already available in workspace:\n"
        "data/c1-asrs\n"
        "Use for unstructured safety/ops narratives in RAG index.\n",
        encoding="utf-8",
    )

    # Build synthetic tables from local source stacks.
    routes_path = latest_file("data/f-openflights/raw/routes_*.dat")
    airports_path = latest_file("data/f-openflights/raw/airports_*.dat")
    opensky_paths = sorted(ROOT.glob("data/e-opensky_recent/opensky_flights_departure_*.json"))

    iata_airports, icao_airports = load_openflights_airports(airports_path)
    routes = load_openflights_routes(routes_path)
    opensky_departures = load_opensky_departures(opensky_paths)
    bag_rate = read_baggage_rate(baggage_csv)
    sdr_jasc = read_sdr_jasc_distribution(sdr_csv)

    counts = synthesize_tables(
        out_dir=out_dir,
        routes=routes,
        iata_airports=iata_airports,
        icao_airports=icao_airports,
        opensky_departures=opensky_departures,
        baggage_rate=bag_rate,
        sdr_jasc_counts=sdr_jasc,
    )

    manifest = {
        "generated_at_utc": ts,
        "output_dir": str(out_dir),
        "raw_files": {
            "bts_baggage_csv": str(baggage_csv),
            "bts_baggage_metadata": str(baggage_meta),
            "bts_ontime_measure_metadata": str(ontime_meta),
            "bts_ontime_access_test": str(ontime_access),
            "faa_sdr_html": str(sdr_html),
            "faa_sdr_extract_csv": str(sdr_csv),
            "faa_sdr_extract_rows": sdr_rows,
            "eurocontrol_coda_page": str(coda_page),
            "eurocontrol_coda_pdf": str(coda_pdf) if coda_pdf.exists() else "",
            "asrs_reference": str(asrs_ref),
        },
        "derived_metrics": {
            "baggage_mishandled_rate": bag_rate,
            "sdr_unique_jasc_codes": len(sdr_jasc),
        },
        "synthetic_counts": counts,
        "notes": [
            "BTS on-time source 56fa-sf82 is a non-tabular measure (resource row access unavailable).",
            "FAA SDR extract is parsed from public query result grid for selected date range.",
            "Synthetic overlays are intended for demo realism, not operational accuracy.",
        ],
    }

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
