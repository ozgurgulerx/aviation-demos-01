#!/usr/bin/env python3
"""
Provision a Fabric IQ Ontology (V2) for the aviation data platform.

Uploads ontology definition parts (8 entity types, 10 relationships) via Fabric REST API:
  - BTSFlights, Airports, DelayAggregates (existing, renamed)
  - Airlines, FlightLegs, CrewDuties, MaintenanceEvents, SafetyReports (new)

IMPORTANT:
  REST provisioning confirms definition storage only. It does not guarantee runtime graph
  schema is queryable. Use the Fabric portal canonical flow after create/update:
    Create/Model -> Publish -> Preview Refresh
  Then run scripts/verify_ontology_runtime.py to validate runtime gates.

Prerequisites:
  - Azure CLI logged in (az login)
  - Lakehouse tables prepared (run notebooks/prepare_ontology_lakehouse.py first)
  - docs/ontology/fabric_ids.json populated with resource IDs

Usage:
  python scripts/provision_fabric_ontology.py [--delete]
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.error
import uuid
from pathlib import Path

IDS_PATH = Path("docs/ontology/fabric_ids.json")
ONTOLOGY_NAME = "AviationOntology"
ONTOLOGY_DESCRIPTION = (
    "Aviation domain ontology (V2) covering BTS flight performance, airports, "
    "carrier delay aggregates, airlines, operational flight legs, crew duties, "
    "maintenance events, and safety reports. 8 entity types, 10 relationships."
)

FULL_TABLE_NAMES = {
    "bts_ontime_reporting": "bts_ontime_reporting",
    "airline_delay_causes": "airline_delay_causes",
    "dim_airports": "dim_airports",
    "dim_airlines": "dim_airlines",
    "ops_flight_legs": "ops_flight_legs",
    "ops_crew_rosters": "ops_crew_rosters",
    "ops_mel_techlog_events": "ops_mel_techlog_events",
    "asrs_reports": "asrs_reports",
}

CANARY_TABLE_SUFFIX = "_canary"

# ── Fabric resource IDs (loaded from fabric_ids.json) ───────────────────────
def load_ids() -> dict:
    if not IDS_PATH.exists():
        print(f"ERROR: {IDS_PATH} not found. Run discover step first.")
        sys.exit(1)
    return json.loads(IDS_PATH.read_text())


# ── Azure auth ──────────────────────────────────────────────────────────────
def get_token() -> str:
    result = subprocess.run(
        ["az", "account", "get-access-token",
         "--resource", "https://api.fabric.microsoft.com",
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


# ── REST helpers ────────────────────────────────────────────────────────────
def fabric_api(method: str, path: str, token: str, body: dict | None = None) -> dict | None:
    import urllib.request
    url = f"https://api.fabric.microsoft.com/v1/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            status = resp.status
            text = resp.read().decode()
            if status == 202:
                # Long-running operation — poll
                location = resp.headers.get("Location", "")
                retry = int(resp.headers.get("Retry-After", "5"))
                return poll_lro(location, token, retry)
            return json.loads(text) if text.strip() else {"status": status}
    except urllib.error.HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        print(f"HTTP {e.code}: {body_text[:500]}")
        raise


def poll_lro(location: str, token: str, retry_after: int = 5) -> dict:
    import urllib.request
    print(f"  LRO polling (retry every {retry_after}s)...", end="", flush=True)
    for _ in range(60):
        time.sleep(retry_after)
        req = urllib.request.Request(location, method="GET")
        req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                status = data.get("status", "Unknown")
                if status in ("Succeeded", "succeeded"):
                    print(f" {status}")
                    return data
                if status in ("Failed", "failed"):
                    print(f" {status}")
                    print(f"  Error: {json.dumps(data, indent=2)[:500]}")
                    raise RuntimeError(f"LRO failed: {data}")
                print(".", end="", flush=True)
        except urllib.error.HTTPError:
            print("x", end="", flush=True)
    raise TimeoutError("LRO timed out after 5 minutes")


# ── Base64 helper ───────────────────────────────────────────────────────────
def b64(obj: dict) -> str:
    return base64.b64encode(json.dumps(obj).encode()).decode()


# ── Entity Type IDs ─────────────────────────────────────────────────────────
# Using sequential BigInts as per the ontology definition spec.

ET_BTS_FLIGHTS = "1000000000001"
ET_AIRPORTS = "1000000000002"
ET_DELAY_AGG = "1000000000003"
ET_AIRLINES = "1000000000004"
ET_FLIGHT_LEGS = "1000000000005"
ET_CREW_DUTIES = "1000000000006"
ET_MAINTENANCE = "1000000000007"
ET_SAFETY = "1000000000008"

# Property IDs — BTSFlights (2000-series)
P_YEAR = "2000000000001"
P_MONTH = "2000000000002"
P_DAY = "2000000000003"
P_AIRLINE_CODE = "2000000000004"
P_FLIGHT_NUM = "2000000000005"
P_FLIGHT_DATE = "2000000000006"
P_ORIGIN = "2000000000007"
P_DEST = "2000000000008"
P_DEP_DELAY = "2000000000009"
P_ARR_DELAY = "2000000000010"
P_DEP_DEL15 = "2000000000011"
P_ARR_DEL15 = "2000000000012"
P_CANCELLED = "2000000000013"
P_CANCEL_CODE = "2000000000014"
P_DIVERTED = "2000000000015"
P_CARRIER_DLY = "2000000000016"
P_WEATHER_DLY = "2000000000017"
P_NAS_DLY = "2000000000018"
P_SEC_DLY = "2000000000019"
P_LATE_AC_DLY = "2000000000020"
P_DISTANCE = "2000000000021"

# Property IDs — Airports (3000-series)
P_IATA = "3000000000001"
P_ICAO = "3000000000002"
P_AIRPORT_NAME = "3000000000003"
P_AIRPORT_TYPE = "3000000000004"
P_LAT = "3000000000005"
P_LON = "3000000000006"
P_ELEV = "3000000000007"
P_COUNTRY = "3000000000008"
P_REGION = "3000000000009"
P_CITY = "3000000000010"

# Property IDs — DelayAggregates (4000-series)
P_DA_CARRIER = "4000000000001"
P_DA_CARRIER_NAME = "4000000000002"
P_DA_YEAR = "4000000000003"
P_DA_MONTH = "4000000000004"
P_DA_AIRPORT = "4000000000005"
P_DA_AIRPORT_NAME = "4000000000006"
P_DA_ARR_FLIGHTS = "4000000000007"
P_DA_ARR_DEL15 = "4000000000008"
P_DA_CARRIER_CT = "4000000000009"
P_DA_WEATHER_CT = "4000000000010"
P_DA_NAS_CT = "4000000000011"
P_DA_SEC_CT = "4000000000012"
P_DA_LATE_AC_CT = "4000000000013"
P_DA_CARRIER_DLY = "4000000000014"
P_DA_WEATHER_DLY = "4000000000015"
P_DA_NAS_DLY = "4000000000016"
P_DA_SEC_DLY = "4000000000017"
P_DA_LATE_AC_DLY = "4000000000018"

# Property IDs — Airlines (5000-series)
P_AL_IATA = "5100000000001"
P_AL_ICAO = "5100000000002"
P_AL_NAME = "5100000000003"
P_AL_CALLSIGN = "5100000000004"
P_AL_COUNTRY = "5100000000005"
P_AL_ACTIVE = "5100000000006"

# Property IDs — FlightLegs (6000-series)
P_FL_LEG_ID = "6000000000001"
P_FL_CARRIER = "6000000000002"
P_FL_FLIGHT_NO = "6000000000003"
P_FL_ORIGIN = "6000000000004"
P_FL_DEST = "6000000000005"
P_FL_DEP_UTC = "6000000000006"
P_FL_ARR_UTC = "6000000000007"
P_FL_TAILNUM = "6000000000008"
P_FL_DISTANCE = "6000000000009"
P_FL_PAX = "6000000000010"

# Property IDs — CrewDuties (7000-series)
P_CD_DUTY_ID = "7000000000001"
P_CD_CREW_ID = "7000000000002"
P_CD_ROLE = "7000000000003"
P_CD_LEG_ID = "7000000000004"
P_CD_START_UTC = "7000000000005"
P_CD_END_UTC = "7000000000006"
P_CD_CUM_HOURS = "7000000000007"
P_CD_RISK_FLAG = "7000000000008"

# Property IDs — MaintenanceEvents (8000-series)
P_ME_EVENT_ID = "8000000000001"
P_ME_LEG_ID = "8000000000002"
P_ME_EVENT_TS = "8000000000003"
P_ME_JASC = "8000000000004"
P_ME_MEL_CAT = "8000000000005"
P_ME_DEFERRED = "8000000000006"
P_ME_SEVERITY = "8000000000007"
P_ME_SOURCE = "8000000000008"
P_ME_NOTE = "8000000000009"

# Property IDs — SafetyReports (9000-series)
P_SR_ID = "9000000000001"
P_SR_DATE = "9000000000002"
P_SR_LOCATION = "9000000000003"
P_SR_AIRCRAFT = "9000000000004"
P_SR_PHASE = "9000000000005"
P_SR_NARRATIVE_TYPE = "9000000000006"
P_SR_TITLE = "9000000000007"
P_SR_TEXT = "9000000000008"
P_SR_INGESTED = "9000000000009"
P_SR_LOCATION_IATA = "9000000000010"

# Relationship Type IDs
RT_DEPARTS = "5000000000001"
RT_ARRIVES = "5000000000002"
RT_OPERATED = "5000000000003"
RT_MARKETED = "5000000000004"
RT_LEG_DEPARTS = "5000000000005"
RT_LEG_ARRIVES = "5000000000006"
RT_CREWED = "5000000000007"
RT_HAS_MAINT = "5000000000008"
RT_FLOWN_BY = "5000000000009"
RT_REPORTED_AT = "5000000000010"


def _prop(pid: str, name: str, vtype: str) -> dict:
    return {
        "id": pid,
        "name": name,
        "redefines": None,
        "baseTypeNamespaceType": None,
        "valueType": vtype,
    }


# ── Entity Type definitions ────────────────────────────────────────────────

def bts_flights_entity() -> dict:
    return {
        "id": ET_BTS_FLIGHTS,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": "BTSFlights",
        "entityIdParts": [P_YEAR, P_MONTH, P_DAY, P_AIRLINE_CODE, P_FLIGHT_NUM],
        "displayNamePropertyId": P_FLIGHT_NUM,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            _prop(P_YEAR, "Year", "BigInt"),
            _prop(P_MONTH, "Month", "BigInt"),
            _prop(P_DAY, "DayofMonth", "BigInt"),
            _prop(P_AIRLINE_CODE, "IATA_Code_Marketing_Airline", "String"),
            _prop(P_FLIGHT_NUM, "Flight_Number_Marketing_Airline", "String"),
            _prop(P_FLIGHT_DATE, "FlightDate", "String"),
            _prop(P_ORIGIN, "Origin", "String"),
            _prop(P_DEST, "Dest", "String"),
            _prop(P_DEP_DELAY, "DepDelayMinutes", "Double"),
            _prop(P_ARR_DELAY, "ArrDelayMinutes", "Double"),
            _prop(P_DEP_DEL15, "DepDel15", "Double"),
            _prop(P_ARR_DEL15, "ArrDel15", "Double"),
            _prop(P_CANCELLED, "Cancelled", "Double"),
            _prop(P_CANCEL_CODE, "CancellationCode", "String"),
            _prop(P_DIVERTED, "Diverted", "Double"),
            _prop(P_CARRIER_DLY, "CarrierDelay", "Double"),
            _prop(P_WEATHER_DLY, "WeatherDelay", "Double"),
            _prop(P_NAS_DLY, "NASDelay", "Double"),
            _prop(P_SEC_DLY, "SecurityDelay", "Double"),
            _prop(P_LATE_AC_DLY, "LateAircraftDelay", "Double"),
            _prop(P_DISTANCE, "Distance", "Double"),
        ],
        "timeseriesProperties": [],
    }


def airports_entity() -> dict:
    return {
        "id": ET_AIRPORTS,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": "Airports",
        "entityIdParts": [P_IATA],
        "displayNamePropertyId": P_AIRPORT_NAME,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            _prop(P_IATA, "iata_code", "String"),
            _prop(P_ICAO, "icao_code", "String"),
            _prop(P_AIRPORT_NAME, "name", "String"),
            _prop(P_AIRPORT_TYPE, "airport_type", "String"),
            _prop(P_LAT, "latitude", "Double"),
            _prop(P_LON, "longitude", "Double"),
            _prop(P_ELEV, "elevation", "Double"),
            _prop(P_COUNTRY, "country", "String"),
            _prop(P_REGION, "region", "String"),
            _prop(P_CITY, "city", "String"),
        ],
        "timeseriesProperties": [],
    }


def delay_aggregates_entity() -> dict:
    return {
        "id": ET_DELAY_AGG,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": "DelayAggregates",
        "entityIdParts": [P_DA_CARRIER, P_DA_YEAR, P_DA_MONTH, P_DA_AIRPORT],
        "displayNamePropertyId": P_DA_CARRIER_NAME,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            _prop(P_DA_CARRIER, "carrier", "String"),
            _prop(P_DA_CARRIER_NAME, "carrier_name", "String"),
            _prop(P_DA_YEAR, "year", "BigInt"),
            _prop(P_DA_MONTH, "month", "BigInt"),
            _prop(P_DA_AIRPORT, "airport", "String"),
            _prop(P_DA_AIRPORT_NAME, "airport_name", "String"),
            _prop(P_DA_ARR_FLIGHTS, "arr_flights", "Double"),
            _prop(P_DA_ARR_DEL15, "arr_del15", "Double"),
            _prop(P_DA_CARRIER_CT, "carrier_ct", "Double"),
            _prop(P_DA_WEATHER_CT, "weather_ct", "Double"),
            _prop(P_DA_NAS_CT, "nas_ct", "Double"),
            _prop(P_DA_SEC_CT, "security_ct", "Double"),
            _prop(P_DA_LATE_AC_CT, "late_aircraft_ct", "Double"),
            _prop(P_DA_CARRIER_DLY, "carrier_delay", "Double"),
            _prop(P_DA_WEATHER_DLY, "weather_delay", "Double"),
            _prop(P_DA_NAS_DLY, "nas_delay", "Double"),
            _prop(P_DA_SEC_DLY, "security_delay", "Double"),
            _prop(P_DA_LATE_AC_DLY, "late_aircraft_delay", "Double"),
        ],
        "timeseriesProperties": [],
    }


def airlines_entity() -> dict:
    return {
        "id": ET_AIRLINES,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": "Airlines",
        "entityIdParts": [P_AL_IATA],
        "displayNamePropertyId": P_AL_NAME,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            _prop(P_AL_IATA, "iata", "String"),
            _prop(P_AL_ICAO, "icao", "String"),
            _prop(P_AL_NAME, "name", "String"),
            _prop(P_AL_CALLSIGN, "callsign", "String"),
            _prop(P_AL_COUNTRY, "country", "String"),
            _prop(P_AL_ACTIVE, "active", "String"),
        ],
        "timeseriesProperties": [],
    }


def flight_legs_entity() -> dict:
    return {
        "id": ET_FLIGHT_LEGS,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": "FlightLegs",
        "entityIdParts": [P_FL_LEG_ID],
        "displayNamePropertyId": P_FL_FLIGHT_NO,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            _prop(P_FL_LEG_ID, "leg_id", "String"),
            _prop(P_FL_CARRIER, "carrier_code", "String"),
            _prop(P_FL_FLIGHT_NO, "flight_no", "String"),
            _prop(P_FL_ORIGIN, "origin_iata", "String"),
            _prop(P_FL_DEST, "dest_iata", "String"),
            _prop(P_FL_DEP_UTC, "scheduled_dep_utc", "String"),
            _prop(P_FL_ARR_UTC, "scheduled_arr_utc", "String"),
            _prop(P_FL_TAILNUM, "tailnum", "String"),
            _prop(P_FL_DISTANCE, "distance_nm", "Double"),
            _prop(P_FL_PAX, "passengers", "BigInt"),
        ],
        "timeseriesProperties": [],
    }


def crew_duties_entity() -> dict:
    return {
        "id": ET_CREW_DUTIES,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": "CrewDuties",
        "entityIdParts": [P_CD_DUTY_ID],
        "displayNamePropertyId": P_CD_CREW_ID,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            _prop(P_CD_DUTY_ID, "duty_id", "String"),
            _prop(P_CD_CREW_ID, "crew_id", "String"),
            _prop(P_CD_ROLE, "role", "String"),
            _prop(P_CD_LEG_ID, "leg_id", "String"),
            _prop(P_CD_START_UTC, "duty_start_utc", "String"),
            _prop(P_CD_END_UTC, "duty_end_utc", "String"),
            _prop(P_CD_CUM_HOURS, "cumulative_duty_hours", "Double"),
            _prop(P_CD_RISK_FLAG, "legality_risk_flag", "BigInt"),
        ],
        "timeseriesProperties": [],
    }


def maintenance_events_entity() -> dict:
    return {
        "id": ET_MAINTENANCE,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": "MaintenanceEvents",
        "entityIdParts": [P_ME_EVENT_ID],
        "displayNamePropertyId": P_ME_JASC,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            _prop(P_ME_EVENT_ID, "tech_event_id", "String"),
            _prop(P_ME_LEG_ID, "leg_id", "String"),
            _prop(P_ME_EVENT_TS, "event_ts_utc", "String"),
            _prop(P_ME_JASC, "jasc_code", "String"),
            _prop(P_ME_MEL_CAT, "mel_category", "String"),
            _prop(P_ME_DEFERRED, "deferred_flag", "BigInt"),
            _prop(P_ME_SEVERITY, "severity", "String"),
            _prop(P_ME_SOURCE, "source_proxy", "String"),
            _prop(P_ME_NOTE, "discrepancy_note", "String"),
        ],
        "timeseriesProperties": [],
    }


def safety_reports_entity() -> dict:
    return {
        "id": ET_SAFETY,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": "SafetyReports",
        "entityIdParts": [P_SR_ID],
        "displayNamePropertyId": P_SR_TITLE,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            _prop(P_SR_ID, "asrs_report_id", "String"),
            _prop(P_SR_DATE, "event_date", "String"),
            _prop(P_SR_LOCATION, "location", "String"),
            _prop(P_SR_AIRCRAFT, "aircraft_type", "String"),
            _prop(P_SR_PHASE, "flight_phase", "String"),
            _prop(P_SR_NARRATIVE_TYPE, "narrative_type", "String"),
            _prop(P_SR_TITLE, "title", "String"),
            _prop(P_SR_TEXT, "report_text", "String"),
            _prop(P_SR_INGESTED, "ingested_at", "String"),
            _prop(P_SR_LOCATION_IATA, "location_iata", "String"),
        ],
        "timeseriesProperties": [],
    }


# ── Data Binding definitions ───────────────────────────────────────────────

def bts_flights_binding(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "bts-flights-binding")),
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": "Year", "targetPropertyId": P_YEAR},
                {"sourceColumnName": "Month", "targetPropertyId": P_MONTH},
                {"sourceColumnName": "DayofMonth", "targetPropertyId": P_DAY},
                {"sourceColumnName": "IATA_Code_Marketing_Airline", "targetPropertyId": P_AIRLINE_CODE},
                {"sourceColumnName": "Flight_Number_Marketing_Airline", "targetPropertyId": P_FLIGHT_NUM},
                {"sourceColumnName": "FlightDate", "targetPropertyId": P_FLIGHT_DATE},
                {"sourceColumnName": "Origin", "targetPropertyId": P_ORIGIN},
                {"sourceColumnName": "Dest", "targetPropertyId": P_DEST},
                {"sourceColumnName": "DepDelayMinutes", "targetPropertyId": P_DEP_DELAY},
                {"sourceColumnName": "ArrDelayMinutes", "targetPropertyId": P_ARR_DELAY},
                {"sourceColumnName": "DepDel15", "targetPropertyId": P_DEP_DEL15},
                {"sourceColumnName": "ArrDel15", "targetPropertyId": P_ARR_DEL15},
                {"sourceColumnName": "Cancelled", "targetPropertyId": P_CANCELLED},
                {"sourceColumnName": "CancellationCode", "targetPropertyId": P_CANCEL_CODE},
                {"sourceColumnName": "Diverted", "targetPropertyId": P_DIVERTED},
                {"sourceColumnName": "CarrierDelay", "targetPropertyId": P_CARRIER_DLY},
                {"sourceColumnName": "WeatherDelay", "targetPropertyId": P_WEATHER_DLY},
                {"sourceColumnName": "NASDelay", "targetPropertyId": P_NAS_DLY},
                {"sourceColumnName": "SecurityDelay", "targetPropertyId": P_SEC_DLY},
                {"sourceColumnName": "LateAircraftDelay", "targetPropertyId": P_LATE_AC_DLY},
                {"sourceColumnName": "Distance", "targetPropertyId": P_DISTANCE},
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": ws_id,
                "itemId": lh_id,
                "sourceTableName": source_table,
                "sourceSchema": "dbo",
            },
        },
    }


def airports_binding(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "airports-binding")),
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": "iata_code", "targetPropertyId": P_IATA},
                {"sourceColumnName": "icao_code", "targetPropertyId": P_ICAO},
                {"sourceColumnName": "name", "targetPropertyId": P_AIRPORT_NAME},
                {"sourceColumnName": "airport_type", "targetPropertyId": P_AIRPORT_TYPE},
                {"sourceColumnName": "latitude", "targetPropertyId": P_LAT},
                {"sourceColumnName": "longitude", "targetPropertyId": P_LON},
                {"sourceColumnName": "elevation", "targetPropertyId": P_ELEV},
                {"sourceColumnName": "country", "targetPropertyId": P_COUNTRY},
                {"sourceColumnName": "region", "targetPropertyId": P_REGION},
                {"sourceColumnName": "city", "targetPropertyId": P_CITY},
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": ws_id,
                "itemId": lh_id,
                "sourceTableName": source_table,
                "sourceSchema": "dbo",
            },
        },
    }


def delay_agg_binding(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "delay-agg-binding")),
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": "carrier", "targetPropertyId": P_DA_CARRIER},
                {"sourceColumnName": "carrier_name", "targetPropertyId": P_DA_CARRIER_NAME},
                {"sourceColumnName": "year", "targetPropertyId": P_DA_YEAR},
                {"sourceColumnName": "month", "targetPropertyId": P_DA_MONTH},
                {"sourceColumnName": "airport", "targetPropertyId": P_DA_AIRPORT},
                {"sourceColumnName": "airport_name", "targetPropertyId": P_DA_AIRPORT_NAME},
                {"sourceColumnName": "arr_flights", "targetPropertyId": P_DA_ARR_FLIGHTS},
                {"sourceColumnName": "arr_del15", "targetPropertyId": P_DA_ARR_DEL15},
                {"sourceColumnName": "carrier_ct", "targetPropertyId": P_DA_CARRIER_CT},
                {"sourceColumnName": "weather_ct", "targetPropertyId": P_DA_WEATHER_CT},
                {"sourceColumnName": "nas_ct", "targetPropertyId": P_DA_NAS_CT},
                {"sourceColumnName": "security_ct", "targetPropertyId": P_DA_SEC_CT},
                {"sourceColumnName": "late_aircraft_ct", "targetPropertyId": P_DA_LATE_AC_CT},
                {"sourceColumnName": "carrier_delay", "targetPropertyId": P_DA_CARRIER_DLY},
                {"sourceColumnName": "weather_delay", "targetPropertyId": P_DA_WEATHER_DLY},
                {"sourceColumnName": "nas_delay", "targetPropertyId": P_DA_NAS_DLY},
                {"sourceColumnName": "security_delay", "targetPropertyId": P_DA_SEC_DLY},
                {"sourceColumnName": "late_aircraft_delay", "targetPropertyId": P_DA_LATE_AC_DLY},
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": ws_id,
                "itemId": lh_id,
                "sourceTableName": source_table,
                "sourceSchema": "dbo",
            },
        },
    }


def airlines_binding(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "airlines-binding")),
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": "iata", "targetPropertyId": P_AL_IATA},
                {"sourceColumnName": "icao", "targetPropertyId": P_AL_ICAO},
                {"sourceColumnName": "name", "targetPropertyId": P_AL_NAME},
                {"sourceColumnName": "callsign", "targetPropertyId": P_AL_CALLSIGN},
                {"sourceColumnName": "country", "targetPropertyId": P_AL_COUNTRY},
                {"sourceColumnName": "active", "targetPropertyId": P_AL_ACTIVE},
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": ws_id,
                "itemId": lh_id,
                "sourceTableName": source_table,
                "sourceSchema": "dbo",
            },
        },
    }


def flight_legs_binding(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "flight-legs-binding")),
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": "leg_id", "targetPropertyId": P_FL_LEG_ID},
                {"sourceColumnName": "carrier_code", "targetPropertyId": P_FL_CARRIER},
                {"sourceColumnName": "flight_no", "targetPropertyId": P_FL_FLIGHT_NO},
                {"sourceColumnName": "origin_iata", "targetPropertyId": P_FL_ORIGIN},
                {"sourceColumnName": "dest_iata", "targetPropertyId": P_FL_DEST},
                {"sourceColumnName": "scheduled_dep_utc", "targetPropertyId": P_FL_DEP_UTC},
                {"sourceColumnName": "scheduled_arr_utc", "targetPropertyId": P_FL_ARR_UTC},
                {"sourceColumnName": "tailnum", "targetPropertyId": P_FL_TAILNUM},
                {"sourceColumnName": "distance_nm", "targetPropertyId": P_FL_DISTANCE},
                {"sourceColumnName": "passengers", "targetPropertyId": P_FL_PAX},
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": ws_id,
                "itemId": lh_id,
                "sourceTableName": source_table,
                "sourceSchema": "dbo",
            },
        },
    }


def crew_duties_binding(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "crew-duties-binding")),
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": "duty_id", "targetPropertyId": P_CD_DUTY_ID},
                {"sourceColumnName": "crew_id", "targetPropertyId": P_CD_CREW_ID},
                {"sourceColumnName": "role", "targetPropertyId": P_CD_ROLE},
                {"sourceColumnName": "leg_id", "targetPropertyId": P_CD_LEG_ID},
                {"sourceColumnName": "duty_start_utc", "targetPropertyId": P_CD_START_UTC},
                {"sourceColumnName": "duty_end_utc", "targetPropertyId": P_CD_END_UTC},
                {"sourceColumnName": "cumulative_duty_hours", "targetPropertyId": P_CD_CUM_HOURS},
                {"sourceColumnName": "legality_risk_flag", "targetPropertyId": P_CD_RISK_FLAG},
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": ws_id,
                "itemId": lh_id,
                "sourceTableName": source_table,
                "sourceSchema": "dbo",
            },
        },
    }


def maintenance_events_binding(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "maintenance-binding")),
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": "tech_event_id", "targetPropertyId": P_ME_EVENT_ID},
                {"sourceColumnName": "leg_id", "targetPropertyId": P_ME_LEG_ID},
                {"sourceColumnName": "event_ts_utc", "targetPropertyId": P_ME_EVENT_TS},
                {"sourceColumnName": "jasc_code", "targetPropertyId": P_ME_JASC},
                {"sourceColumnName": "mel_category", "targetPropertyId": P_ME_MEL_CAT},
                {"sourceColumnName": "deferred_flag", "targetPropertyId": P_ME_DEFERRED},
                {"sourceColumnName": "severity", "targetPropertyId": P_ME_SEVERITY},
                {"sourceColumnName": "source_proxy", "targetPropertyId": P_ME_SOURCE},
                {"sourceColumnName": "discrepancy_note", "targetPropertyId": P_ME_NOTE},
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": ws_id,
                "itemId": lh_id,
                "sourceTableName": source_table,
                "sourceSchema": "dbo",
            },
        },
    }


def safety_reports_binding(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "safety-reports-binding")),
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": "asrs_report_id", "targetPropertyId": P_SR_ID},
                {"sourceColumnName": "event_date", "targetPropertyId": P_SR_DATE},
                {"sourceColumnName": "location", "targetPropertyId": P_SR_LOCATION},
                {"sourceColumnName": "aircraft_type", "targetPropertyId": P_SR_AIRCRAFT},
                {"sourceColumnName": "flight_phase", "targetPropertyId": P_SR_PHASE},
                {"sourceColumnName": "narrative_type", "targetPropertyId": P_SR_NARRATIVE_TYPE},
                {"sourceColumnName": "title", "targetPropertyId": P_SR_TITLE},
                {"sourceColumnName": "report_text", "targetPropertyId": P_SR_TEXT},
                {"sourceColumnName": "ingested_at", "targetPropertyId": P_SR_INGESTED},
                {"sourceColumnName": "location_iata", "targetPropertyId": P_SR_LOCATION_IATA},
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": ws_id,
                "itemId": lh_id,
                "sourceTableName": source_table,
                "sourceSchema": "dbo",
            },
        },
    }


# ── Relationship Type definitions ──────────────────────────────────────────

def departs_from_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_DEPARTS,
        "name": "departsFrom",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_BTS_FLIGHTS},
        "target": {"entityTypeId": ET_AIRPORTS},
    }


def arrives_at_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_ARRIVES,
        "name": "arrivesAt",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_BTS_FLIGHTS},
        "target": {"entityTypeId": ET_AIRPORTS},
    }


def operated_by_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_OPERATED,
        "name": "operatedBy",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_BTS_FLIGHTS},
        "target": {"entityTypeId": ET_DELAY_AGG},
    }


def marketed_by_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_MARKETED,
        "name": "marketedBy",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_BTS_FLIGHTS},
        "target": {"entityTypeId": ET_AIRLINES},
    }


def leg_departs_from_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_LEG_DEPARTS,
        "name": "legDepartsFrom",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_FLIGHT_LEGS},
        "target": {"entityTypeId": ET_AIRPORTS},
    }


def leg_arrives_at_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_LEG_ARRIVES,
        "name": "legArrivesAt",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_FLIGHT_LEGS},
        "target": {"entityTypeId": ET_AIRPORTS},
    }


def crewed_by_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_CREWED,
        "name": "crewedBy",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_FLIGHT_LEGS},
        "target": {"entityTypeId": ET_CREW_DUTIES},
    }


def has_maintenance_event_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_HAS_MAINT,
        "name": "hasMaintenanceEvent",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_FLIGHT_LEGS},
        "target": {"entityTypeId": ET_MAINTENANCE},
    }


def flown_by_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_FLOWN_BY,
        "name": "flownBy",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_FLIGHT_LEGS},
        "target": {"entityTypeId": ET_AIRLINES},
    }


def reported_at_rel() -> dict:
    return {
        "namespace": "usertypes",
        "id": RT_REPORTED_AT,
        "name": "reportedAt",
        "namespaceType": "Custom",
        "source": {"entityTypeId": ET_SAFETY},
        "target": {"entityTypeId": ET_AIRPORTS},
    }


# ── Relationship Contextualizations (join table bindings) ──────────────────

def departs_from_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "departs-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "Year", "targetPropertyId": P_YEAR},
            {"sourceColumnName": "Month", "targetPropertyId": P_MONTH},
            {"sourceColumnName": "DayofMonth", "targetPropertyId": P_DAY},
            {"sourceColumnName": "IATA_Code_Marketing_Airline", "targetPropertyId": P_AIRLINE_CODE},
            {"sourceColumnName": "Flight_Number_Marketing_Airline", "targetPropertyId": P_FLIGHT_NUM},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "Origin", "targetPropertyId": P_IATA},
        ],
    }


def arrives_at_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "arrives-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "Year", "targetPropertyId": P_YEAR},
            {"sourceColumnName": "Month", "targetPropertyId": P_MONTH},
            {"sourceColumnName": "DayofMonth", "targetPropertyId": P_DAY},
            {"sourceColumnName": "IATA_Code_Marketing_Airline", "targetPropertyId": P_AIRLINE_CODE},
            {"sourceColumnName": "Flight_Number_Marketing_Airline", "targetPropertyId": P_FLIGHT_NUM},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "Dest", "targetPropertyId": P_IATA},
        ],
    }


def operated_by_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "operated-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "Year", "targetPropertyId": P_YEAR},
            {"sourceColumnName": "Month", "targetPropertyId": P_MONTH},
            {"sourceColumnName": "DayofMonth", "targetPropertyId": P_DAY},
            {"sourceColumnName": "IATA_Code_Marketing_Airline", "targetPropertyId": P_AIRLINE_CODE},
            {"sourceColumnName": "Flight_Number_Marketing_Airline", "targetPropertyId": P_FLIGHT_NUM},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "IATA_Code_Marketing_Airline", "targetPropertyId": P_DA_CARRIER},
            {"sourceColumnName": "Year", "targetPropertyId": P_DA_YEAR},
            {"sourceColumnName": "Month", "targetPropertyId": P_DA_MONTH},
            {"sourceColumnName": "Origin", "targetPropertyId": P_DA_AIRPORT},
        ],
    }


def marketed_by_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "marketed-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "Year", "targetPropertyId": P_YEAR},
            {"sourceColumnName": "Month", "targetPropertyId": P_MONTH},
            {"sourceColumnName": "DayofMonth", "targetPropertyId": P_DAY},
            {"sourceColumnName": "IATA_Code_Marketing_Airline", "targetPropertyId": P_AIRLINE_CODE},
            {"sourceColumnName": "Flight_Number_Marketing_Airline", "targetPropertyId": P_FLIGHT_NUM},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "IATA_Code_Marketing_Airline", "targetPropertyId": P_AL_IATA},
        ],
    }


def leg_departs_from_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "leg-departs-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "leg_id", "targetPropertyId": P_FL_LEG_ID},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "origin_iata", "targetPropertyId": P_IATA},
        ],
    }


def leg_arrives_at_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "leg-arrives-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "leg_id", "targetPropertyId": P_FL_LEG_ID},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "dest_iata", "targetPropertyId": P_IATA},
        ],
    }


def crewed_by_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "crewed-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "leg_id", "targetPropertyId": P_FL_LEG_ID},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "duty_id", "targetPropertyId": P_CD_DUTY_ID},
        ],
    }


def has_maintenance_event_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "has-maint-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "leg_id", "targetPropertyId": P_FL_LEG_ID},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "tech_event_id", "targetPropertyId": P_ME_EVENT_ID},
        ],
    }


def flown_by_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "flown-by-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "leg_id", "targetPropertyId": P_FL_LEG_ID},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "carrier_code", "targetPropertyId": P_AL_IATA},
        ],
    }


def reported_at_ctx(ws_id: str, lh_id: str, source_table: str) -> dict:
    return {
        "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "reported-at-ctx")),
        "dataBindingTable": {
            "workspaceId": ws_id,
            "itemId": lh_id,
            "sourceTableName": source_table,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": "asrs_report_id", "targetPropertyId": P_SR_ID},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": "location_iata", "targetPropertyId": P_IATA},
        ],
    }


# ── Build the full ontology definition ─────────────────────────────────────

def resolve_table_names(ids: dict, binding_profile: str) -> dict[str, str]:
    if binding_profile == "full":
        return dict(FULL_TABLE_NAMES)

    canary = ids.get("canaryTables")
    if isinstance(canary, dict):
        table_names = {}
        for key, full_name in FULL_TABLE_NAMES.items():
            candidate = canary.get(key)
            table_names[key] = str(candidate).strip() if candidate else f"{full_name}{CANARY_TABLE_SUFFIX}"
        return table_names

    return {
        key: f"{full_name}{CANARY_TABLE_SUFFIX}" for key, full_name in FULL_TABLE_NAMES.items()
    }


def build_definition(ws_id: str, lh_id: str, table_names: dict[str, str]) -> dict:
    """Build the complete V2 ontology definition as Fabric REST API expects it."""
    platform = {
        "metadata": {
            "type": "Ontology",
            "displayName": ONTOLOGY_NAME,
        }
    }

    # Bindings
    fb = bts_flights_binding(ws_id, lh_id, table_names["bts_ontime_reporting"])
    ab = airports_binding(ws_id, lh_id, table_names["dim_airports"])
    db = delay_agg_binding(ws_id, lh_id, table_names["airline_delay_causes"])
    alb = airlines_binding(ws_id, lh_id, table_names["dim_airlines"])
    flb = flight_legs_binding(ws_id, lh_id, table_names["ops_flight_legs"])
    cdb = crew_duties_binding(ws_id, lh_id, table_names["ops_crew_rosters"])
    meb = maintenance_events_binding(ws_id, lh_id, table_names["ops_mel_techlog_events"])
    srb = safety_reports_binding(ws_id, lh_id, table_names["asrs_reports"])

    # Contextualizations
    dc = departs_from_ctx(ws_id, lh_id, table_names["bts_ontime_reporting"])
    ac = arrives_at_ctx(ws_id, lh_id, table_names["bts_ontime_reporting"])
    oc = operated_by_ctx(ws_id, lh_id, table_names["bts_ontime_reporting"])
    mc = marketed_by_ctx(ws_id, lh_id, table_names["bts_ontime_reporting"])
    ldc = leg_departs_from_ctx(ws_id, lh_id, table_names["ops_flight_legs"])
    lac = leg_arrives_at_ctx(ws_id, lh_id, table_names["ops_flight_legs"])
    cc = crewed_by_ctx(ws_id, lh_id, table_names["ops_crew_rosters"])
    hmc = has_maintenance_event_ctx(ws_id, lh_id, table_names["ops_mel_techlog_events"])
    fbc = flown_by_ctx(ws_id, lh_id, table_names["ops_flight_legs"])
    rac = reported_at_ctx(ws_id, lh_id, table_names["asrs_reports"])

    parts = [
        {"path": ".platform", "payload": b64(platform), "payloadType": "InlineBase64"},
        {"path": "definition.json", "payload": b64({}), "payloadType": "InlineBase64"},
        # Entity Types + Data Bindings
        {"path": f"EntityTypes/{ET_BTS_FLIGHTS}/definition.json",
         "payload": b64(bts_flights_entity()), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_BTS_FLIGHTS}/DataBindings/{fb['id']}.json",
         "payload": b64(fb), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_AIRPORTS}/definition.json",
         "payload": b64(airports_entity()), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_AIRPORTS}/DataBindings/{ab['id']}.json",
         "payload": b64(ab), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_DELAY_AGG}/definition.json",
         "payload": b64(delay_aggregates_entity()), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_DELAY_AGG}/DataBindings/{db['id']}.json",
         "payload": b64(db), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_AIRLINES}/definition.json",
         "payload": b64(airlines_entity()), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_AIRLINES}/DataBindings/{alb['id']}.json",
         "payload": b64(alb), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_FLIGHT_LEGS}/definition.json",
         "payload": b64(flight_legs_entity()), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_FLIGHT_LEGS}/DataBindings/{flb['id']}.json",
         "payload": b64(flb), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_CREW_DUTIES}/definition.json",
         "payload": b64(crew_duties_entity()), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_CREW_DUTIES}/DataBindings/{cdb['id']}.json",
         "payload": b64(cdb), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_MAINTENANCE}/definition.json",
         "payload": b64(maintenance_events_entity()), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_MAINTENANCE}/DataBindings/{meb['id']}.json",
         "payload": b64(meb), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_SAFETY}/definition.json",
         "payload": b64(safety_reports_entity()), "payloadType": "InlineBase64"},
        {"path": f"EntityTypes/{ET_SAFETY}/DataBindings/{srb['id']}.json",
         "payload": b64(srb), "payloadType": "InlineBase64"},
        # Relationship Types + Contextualizations
        {"path": f"RelationshipTypes/{RT_DEPARTS}/definition.json",
         "payload": b64(departs_from_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_DEPARTS}/Contextualizations/{dc['id']}.json",
         "payload": b64(dc), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_ARRIVES}/definition.json",
         "payload": b64(arrives_at_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_ARRIVES}/Contextualizations/{ac['id']}.json",
         "payload": b64(ac), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_OPERATED}/definition.json",
         "payload": b64(operated_by_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_OPERATED}/Contextualizations/{oc['id']}.json",
         "payload": b64(oc), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_MARKETED}/definition.json",
         "payload": b64(marketed_by_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_MARKETED}/Contextualizations/{mc['id']}.json",
         "payload": b64(mc), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_LEG_DEPARTS}/definition.json",
         "payload": b64(leg_departs_from_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_LEG_DEPARTS}/Contextualizations/{ldc['id']}.json",
         "payload": b64(ldc), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_LEG_ARRIVES}/definition.json",
         "payload": b64(leg_arrives_at_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_LEG_ARRIVES}/Contextualizations/{lac['id']}.json",
         "payload": b64(lac), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_CREWED}/definition.json",
         "payload": b64(crewed_by_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_CREWED}/Contextualizations/{cc['id']}.json",
         "payload": b64(cc), "payloadType": "InlineBase64"},
        # New relationships: hasMaintenanceEvent, flownBy, reportedAt
        {"path": f"RelationshipTypes/{RT_HAS_MAINT}/definition.json",
         "payload": b64(has_maintenance_event_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_HAS_MAINT}/Contextualizations/{hmc['id']}.json",
         "payload": b64(hmc), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_FLOWN_BY}/definition.json",
         "payload": b64(flown_by_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_FLOWN_BY}/Contextualizations/{fbc['id']}.json",
         "payload": b64(fbc), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_REPORTED_AT}/definition.json",
         "payload": b64(reported_at_rel()), "payloadType": "InlineBase64"},
        {"path": f"RelationshipTypes/{RT_REPORTED_AT}/Contextualizations/{rac['id']}.json",
         "payload": b64(rac), "payloadType": "InlineBase64"},
    ]

    return {"definition": {"parts": parts}}


# ── Main ────────────────────────────────────────────────────────────────────

def list_ontologies(token: str, ws_id: str) -> list:
    result = fabric_api("GET", f"workspaces/{ws_id}/ontologies", token)
    return result.get("value", []) if result else []


def delete_ontology(token: str, ws_id: str, ont_id: str):
    print(f"Deleting ontology {ont_id}...")
    fabric_api("DELETE", f"workspaces/{ws_id}/ontologies/{ont_id}", token)
    print("  Deleted.")


def wait_for_ontology_absence(
    token: str, ws_id: str, name: str, timeout_seconds: int, poll_seconds: int = 10
) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() <= deadline:
        existing = list_ontologies(token, ws_id)
        if not any(str(ont.get("displayName", "")) == name for ont in existing):
            return True
        time.sleep(poll_seconds)
    return False


def create_ontology(
    token: str,
    ws_id: str,
    lh_id: str,
    ids: dict,
    binding_profile: str,
) -> dict:
    table_names = resolve_table_names(ids, binding_profile)
    body = {
        "displayName": ONTOLOGY_NAME,
        "description": ONTOLOGY_DESCRIPTION,
    }
    body.update(build_definition(ws_id, lh_id, table_names))

    print(f"Creating ontology '{ONTOLOGY_NAME}' (V2)...")
    print("  NOTE: API create/update stores definition parts only; runtime validation is separate.")
    print(f"  Binding profile: {binding_profile}")
    print(f"  Definition: {len(body['definition']['parts'])} parts")
    print(f"    8 entity types: BTSFlights, Airports, DelayAggregates, Airlines,")
    print(f"                    FlightLegs, CrewDuties, MaintenanceEvents, SafetyReports")
    print(f"    8 data bindings: bts_ontime_reporting, dim_airports, airline_delay_causes,")
    print(f"                     dim_airlines, ops_flight_legs, ops_crew_rosters,")
    print(f"                     ops_mel_techlog_events, asrs_reports")
    print(f"    7 relationships: departsFrom, arrivesAt, operatedBy, marketedBy,")
    print(f"                     legDepartsFrom, legArrivesAt, crewedBy")
    print(f"    7 contextualizations")
    print("  Source tables:")
    for key in sorted(table_names.keys()):
        print(f"    - {key}: {table_names[key]}")

    result = fabric_api("POST", f"workspaces/{ws_id}/ontologies", token, body)
    ontology_id = ""
    if isinstance(result, dict):
        ontology_id = str(result.get("id") or result.get("resourceId") or "").strip()
    if not ontology_id:
        existing = list_ontologies(token, ws_id)
        matches = [ont for ont in existing if ont.get("displayName") == ONTOLOGY_NAME]
        if matches:
            ontology_id = str(matches[0].get("id") or "").strip()
    if ontology_id:
        return {"id": ontology_id, "rawResult": result}
    return {"rawResult": result}


def main():
    parser = argparse.ArgumentParser(description="Provision Fabric IQ Ontology (V2)")
    parser.add_argument("--delete", action="store_true",
                        help="Delete existing ontology before creating")
    parser.add_argument("--delete-only", action="store_true",
                        help="Only delete existing ontology, don't create")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print definition JSON without creating")
    parser.add_argument(
        "--binding-profile",
        choices=["full", "canary"],
        default="full",
        help="Select source-table binding profile. 'canary' uses canaryTables or *_canary table names.",
    )
    parser.add_argument(
        "--consistency-wait-seconds",
        type=int,
        default=300,
        help="Max seconds to wait for ontology delete eventual consistency before recreate.",
    )
    args = parser.parse_args()

    ids = load_ids()
    ws_id = ids["workspace"]["id"]
    lh_id = ids["lakehouse"]["id"]

    if args.dry_run:
        table_names = resolve_table_names(ids, args.binding_profile)
        defn = build_definition(ws_id, lh_id, table_names)
        print(json.dumps(defn, indent=2))
        return

    token = get_token()

    # Check for existing ontologies
    existing = list_ontologies(token, ws_id)
    print(f"Existing ontologies: {len(existing)}")
    for ont in existing:
        print(f"  {ont['displayName']} ({ont['id']})")

    # Delete if requested
    if args.delete or args.delete_only:
        deleted_any = False
        for ont in existing:
            if ont["displayName"] == ONTOLOGY_NAME:
                delete_ontology(token, ws_id, ont["id"])
                deleted_any = True
        if deleted_any:
            print(
                f"Waiting for delete consistency (up to {args.consistency_wait_seconds}s)..."
            )
            consistent = wait_for_ontology_absence(
                token,
                ws_id,
                ONTOLOGY_NAME,
                timeout_seconds=max(1, args.consistency_wait_seconds),
            )
            if not consistent:
                raise RuntimeError(
                    "Ontology still appears in list after delete wait window. "
                    "Retry with a larger --consistency-wait-seconds value."
                )
        if args.delete_only:
            return

    # Check if already exists
    for ont in existing:
        if ont["displayName"] == ONTOLOGY_NAME:
            print(f"\nOntology '{ONTOLOGY_NAME}' already exists: {ont['id']}")
            print("Use --delete to recreate.")
            return

    # Create
    result = create_ontology(
        token,
        ws_id,
        lh_id,
        ids=ids,
        binding_profile=args.binding_profile,
    )
    print(f"\nOntology created successfully!")
    if result:
        ont_id = result.get("id", result.get("resourceId", "unknown"))
        print(f"  ID: {ont_id}")
        print(f"  Name: {ONTOLOGY_NAME}")
        print(f"\nNext steps:")
        print("  1. Open ontology in Fabric portal -> verify model, then click Publish")
        print("  2. Open ontology Preview -> run Refresh and wait for completion")
        print("  3. Run: python scripts/verify_ontology_runtime.py")
        print("  4. Only after gates pass, run configure-data-agent notebook in Fabric")


if __name__ == "__main__":
    main()
