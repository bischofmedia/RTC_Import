#!/usr/bin/env python3
"""
rtc_import_results_s345.py
──────────────────────────
Importiert Rennergebnisse fuer RTC-Saisons 3, 4 und 5.

Sheet-Aufbau:
  - "Races":    Alle Rennergebnisse in einem Sheet, Rennen nebeneinander
  - "Penalties": Strafen pro Fahrer und Rennen
  - "Streams":  Rennkalender (Season 4+5), bei Season 3 aus DB

Format-Unterschiede:
  Season 5 (ID=5): Offset +17, Boni: POD/FL/FT/SR,    Kalender: Streams (C/D/E)
  Season 4 (ID=4): Offset +17, Boni: POD/FL/FT/SR,    Kalender: Streams (B/C/D+E)
  Season 3 (ID=3): Offset +18, Boni: POD/FL/FT/SR/LP, Kalender: aus DB

Verwendung:
  python3 rtc_import_results_s345.py --season 5
  python3 rtc_import_results_s345.py --season 5 --race 3
"""

import os
import re
import sys
import json
import logging
import argparse
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pymysql
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Konfiguration ─────────────────────────────────────────────────────────────

ENV_PATH = "/etc/RTC_RaceResultBot-env"
load_dotenv(ENV_PATH)

DB_HOST        = os.getenv("DB_HOST")
DB_USER        = os.getenv("DB_USER")
DB_PASSWORD    = os.getenv("DB_PASSWORD")
DB_NAME        = os.getenv("DB_NAME")
CREDS_PATH     = os.getenv("GOOGLE_CREDENTIALS")
DISCORD_TOKEN  = os.getenv("DISCORD_TOKEN_DATABASEBOT")
DISCORD_LOG_CH = os.getenv("DISCORD_CHANNEL_DATABASELOG")
BERLIN         = ZoneInfo("Europe/Berlin")

BONUS_FL  = "FL"
BONUS_POD = "POD"
BONUS_FT  = "FT"
BONUS_SR  = "SR"
BONUS_LP  = "LP"

DNF_MARKER = "8:00:00"

# Format-Definitionen pro Season
SEASON_FORMATS = {
    5: {"offset": 17, "boni": [BONUS_POD, BONUS_FL, BONUS_FT, BONUS_SR], "calendar": "streams_s5"},
    4: {"offset": 17, "boni": [BONUS_POD, BONUS_FL, BONUS_FT, BONUS_SR], "calendar": "streams_s4"},
    3: {"offset": 18, "boni": [BONUS_POD, BONUS_FL, BONUS_FT, BONUS_SR, BONUS_LP], "calendar": "db"},
}

# Spalten-Indizes innerhalb eines Renn-Blocks (0-basiert, relativ zum Block-Start)
# B=1: Pos, C=2: Driver, E=4: Fahrzeug, G=6: RaceTime,
# I=8: Team, K=10: Grid, L=11: Punkte ohne Bonus
COL_POS      = 1   # B
COL_DRIVER   = 2   # C
COL_VEHICLE  = 4   # E
COL_TIME     = 6   # G
COL_TEAM     = 8   # I
COL_GRID     = 10  # K
COL_PTS_BASE = 11  # L

# Boni-Spalten relativ zum Block-Start:
# Season 4+5: M=12 POD, N=13 FL, O=14 FT, P=15 SR, Q=16 Gesamt
# Season 3:   M=12 POD, N=13 FL, O=14 LP, P=15 FT, Q=16 SR, R=17 Gesamt
BONI_COLS_S45 = {BONUS_POD: 12, BONUS_FL: 13, BONUS_FT: 14, BONUS_SR: 15}
BONI_COLS_S3  = {BONUS_POD: 12, BONUS_FL: 13, BONUS_LP: 14, BONUS_FT: 15, BONUS_SR: 16}
COL_PTS_TOTAL_S45 = 16
COL_PTS_TOTAL_S3  = 17

# FL-Zeile: Zeile 4 (Index 3), relativ zum Sheet
FL_ROW = 3
# FL Driver: Block-Start + 2 (Spalte C), FL Zeit: Block-Start + 6 (Spalte G)
FL_COL_DRIVER = 2
FL_COL_TIME   = 6

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "rtc_import_results_s345.log")
        ),
    ],
)
log = logging.getLogger("rtc_import_s345")


# ── Datenbank ─────────────────────────────────────────────────────────────────

def get_db():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
    )


# ── Google Sheets ─────────────────────────────────────────────────────────────

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def fetch_sheet(service, sheet_id, tab):
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range=f"'{tab}'")
        .execute()
    )
    return result.get("values", [])


def list_sheet_tabs(service, sheet_id):
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    return [s["properties"]["title"] for s in meta["sheets"]]


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def cell(row, idx, default=""):
    try:
        return str(row[idx]).strip()
    except (IndexError, TypeError):
        return default


def parse_time_to_seconds(t):
    if not t:
        return None
    # Komma oder Punkt als Dezimaltrenner → Punkt
    t = t.replace(",", ".").replace(".", ".", 10)
    # Letztes Trennzeichen (Millisekunden) normalisieren:
    # Format kann sein 0:51:09.608 oder 0:51:09,608
    parts = t.split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        else:
            return float(parts[0])
    except (ValueError, IndexError):
        return None


def seconds_to_timestr(sec):
    if sec is None:
        return None
    h  = int(sec // 3600)
    m  = int((sec % 3600) // 60)
    s  = sec % 60
    ms = round((s - int(s)) * 1000)
    if ms == 1000:
        ms = 0
        s_int = int(s) + 1
    else:
        s_int = int(s)
    return f"{h}:{m:02d}:{s_int:02d},{ms:03d}"


def col_letter_to_index(letters):
    """Konvertiert Spaltenbuchstaben in 0-basierten Index. B=1, T=19, AL=37 etc."""
    letters = letters.upper()
    result = 0
    for ch in letters:
        result = result * 26 + (ord(ch) - ord('A') + 1)
    return result - 1


# ── DB-Lookups ────────────────────────────────────────────────────────────────

# Fahrzeugname-Übersetzungen: Sheet-Name → DB-Name
VEHICLE_NAME_MAP = {
    "Volkswagen BEETLE":     "VW BEETLE",
    "Volkswagen GTI VGT":    "VW GTI VGT",
    "Chevrolet CORVETTE C7": "Corvette C7",
    "Mercedes-Benz AMG":     "Mercedes AMG",
    "Mercedes-Benz SLS AMG": "Mercedes SLS",
    "Renault Sport R.S.01":  "Renault R.S.01",
    "Alfa Romeo 4C":         "Alfa 4C",
    "Dodge VIPER SRT":       "Dodge Viper",
    "Ferrari 458 ITALIA":    "Ferrari 458",
    "Mitsubishi LANCER EVO": "Mitsubishi Lancer",
    "Nissan GT-R NISMO":     "Nissan GT-R '13",
    "Lamborghini HURACAN":   "Lamborghini '15",
    "Ford GT LM SPEC II":    "Ford GT LM",
    "BMW M6":                "BMW M6",
    "BMW M3 GT":             "BMW M3 '11",
    "Audi R8 LMS":           "Audi R8 LMS '15",
}

# Teamname-Übersetzungen: Sheet-Name → DB-Name
TEAM_NAME_MAP = {
    "Team Coyote":          "Racing Team Coyote",
    "Noller Racing":        "Noller Racing Team",
    "TFD Racing":           "TFD Racing Team",
    "NRT":                  "NRT",           # team_id=197
    "Narcotic Racing Club": "Narcotic Racing Club",  # team_id=198
    "Narcotic Racin Club":  "Narcotic Racing Club",  # Schreibfehler-Variante
}

def lookup_driver(cur, psn_name):
    cur.execute("SELECT driver_id FROM drivers WHERE psn_name = %s", (psn_name,))
    row = cur.fetchone()
    return row["driver_id"] if row else None


def lookup_or_create_driver(cur, psn_name):
    did = lookup_driver(cur, psn_name)
    if did:
        return did
    log.info(f"  Neuer Fahrer angelegt: {psn_name}")
    cur.execute("INSERT INTO drivers (psn_name, is_active) VALUES (%s, 1)", (psn_name,))
    return cur.lastrowid


def lookup_team(cur, name):
    if not name:
        return None
    # Übersetzung anwenden falls vorhanden
    name = TEAM_NAME_MAP.get(name, name)
    cur.execute("SELECT team_id FROM teams WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        return row["team_id"]
    cur.execute("SELECT team_id, name FROM teams WHERE name LIKE %s LIMIT 1",
                (f"%{name}%",))
    row = cur.fetchone()
    if row:
        log.warning(f"  Team fuzzy: '{name}' → '{row['name']}'")
        return row["team_id"]
    log.warning(f"  Team nicht gefunden: '{name}'")
    return None


def lookup_vehicle(cur, name):
    if not name:
        return None
    # Übersetzung anwenden falls vorhanden
    name = VEHICLE_NAME_MAP.get(name, name)
    cur.execute("SELECT vehicle_id FROM vehicles WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        return row["vehicle_id"]
    cur.execute("SELECT vehicle_id, name FROM vehicles WHERE name LIKE %s LIMIT 1",
                (f"%{name[:12]}%",))
    row = cur.fetchone()
    if row:
        log.warning(f"  Fahrzeug fuzzy: '{name}' → '{row['name']}'")
        return row["vehicle_id"]
    log.warning(f"  Fahrzeug nicht gefunden: '{name}'")
    return None


def lookup_track(cur, sheet_name):
    if not sheet_name:
        return None
    cur.execute("SELECT track_id FROM tracks WHERE sheet_name = %s", (sheet_name,))
    row = cur.fetchone()
    if row:
        return row["track_id"]
    cur.execute("SELECT track_id FROM tracks WHERE TRIM(sheet_name) = %s", (sheet_name.strip(),))
    row = cur.fetchone()
    if row:
        return row["track_id"]
    log.warning(f"  Strecke nicht gefunden: '{sheet_name}'")
    return None


def lookup_version_id(cur, race_date):
    if not race_date:
        cur.execute(
            "SELECT version_id FROM game_versions WHERE game = 'Gran Turismo Sport' "
            "ORDER BY release_date DESC LIMIT 1"
        )
    else:
        cur.execute(
            "SELECT version_id FROM game_versions "
            "WHERE game IN ('Gran Turismo Sport', 'Gran Turismo 7') "
            "AND release_date <= %s ORDER BY release_date DESC LIMIT 1",
            (race_date,)
        )
    row = cur.fetchone()
    return row["version_id"] if row else 1


def lookup_grid(cur, race_id, grid_number):
    cur.execute(
        "SELECT grid_id FROM grids WHERE race_id = %s AND grid_number = %s",
        (race_id, grid_number),
    )
    row = cur.fetchone()
    return row["grid_id"] if row else None


def ensure_grids(cur, race_id, grid_numbers):
    GRID_LABELS = {
        "1":  ("1",  "Grid 1"),
        "2":  ("2",  "Grid 2"),
        "2a": ("2",  "Grid 2a"),
        "2b": ("2",  "Grid 2b"),
        "2c": ("2",  "Grid 2c"),
        "3":  ("3",  "Grid 3"),
    }
    for gn in sorted(grid_numbers):
        cur.execute(
            "SELECT grid_id FROM grids WHERE race_id = %s AND grid_number = %s",
            (race_id, gn),
        )
        if cur.fetchone():
            continue
        grid_class, grid_label = GRID_LABELS.get(gn, (gn, f"Grid {gn}"))
        cur.execute(
            "INSERT INTO grids (race_id, grid_number, grid_class, grid_label) "
            "VALUES (%s, %s, %s, %s)",
            (race_id, gn, grid_class, grid_label),
        )
        log.info(f"  Grid '{gn}' fuer race_id={race_id} angelegt.")


def lookup_or_create_race(cur, season_id, race_number, cal_entry):
    cur.execute(
        "SELECT race_id, version_id FROM races WHERE season_id = %s AND race_number = %s",
        (season_id, race_number),
    )
    row = cur.fetchone()

    race_date    = cal_entry.get("race_date")
    track_name   = cal_entry.get("track_name", "")
    laps         = cal_entry.get("laps")
    time_of_day  = cal_entry.get("time_of_day")
    weather_code = cal_entry.get("weather_code")
    correct_vid  = lookup_version_id(cur, race_date)

    if row:
        race_id = row["race_id"]
        if correct_vid != row["version_id"]:
            cur.execute(
                "UPDATE races SET version_id = %s WHERE race_id = %s",
                (correct_vid, race_id),
            )
            log.info(f"  Rennen {race_number}: version_id {row['version_id']} → {correct_vid}")
        return race_id

    track_id = lookup_track(cur, track_name)
    if not track_id and track_name:
        log.error(f"  Rennen {race_number}: Strecke '{track_name}' nicht gefunden.")
        return None

    cur.execute(
        """INSERT INTO races
           (season_id, track_id, version_id, race_number, race_date,
            laps, time_of_day, weather_code)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        (season_id, track_id, correct_vid, race_number, race_date,
         laps, time_of_day, weather_code),
    )
    race_id = cur.lastrowid
    log.info(f"  Rennen {race_number} neu angelegt: race_id={race_id}, "
             f"track_id={track_id}, date={race_date}")
    return race_id


# ── Kalender-Parsing ──────────────────────────────────────────────────────────

def parse_streams_s5(rows):
    """Season 5: C=Rennnummer, D=Datum, E=Strecke. Daten ab Zeile 4 (Index 3)."""
    cal = {}
    for row in rows[3:]:
        rn_raw = cell(row, 2)
        if not rn_raw or not rn_raw.isdigit():
            continue
        rn        = int(rn_raw)
        date_raw  = cell(row, 3)
        race_date = None
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                race_date = datetime.strptime(date_raw, fmt).date()
                break
            except ValueError:
                pass
        cal[rn] = {
            "race_date":    race_date,
            "track_name":   cell(row, 4),
            "laps":         None,
            "time_of_day":  None,
            "weather_code": None,
        }
    log.info(f"Streams (S5): {len(cal)} Rennen gefunden.")
    return cal


def parse_streams_s4(rows):
    """Season 4: B=Rennnummer, C=Datum, D=Strecke, E=Tageszeit+Wetter. Daten ab Zeile 4 (Index 3)."""
    cal = {}
    for row in rows[3:]:
        rn_raw = cell(row, 1)
        if not rn_raw or not rn_raw.isdigit():
            continue
        rn        = int(rn_raw)
        date_raw  = cell(row, 2)
        track     = cell(row, 3)
        tod_raw   = cell(row, 4)

        race_date = None
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                race_date = datetime.strptime(date_raw, fmt).date()
                break
            except ValueError:
                pass

        time_of_day  = None
        weather_code = None
        if tod_raw:
            parts = tod_raw.split(" ", 1)
            time_of_day  = parts[0] if parts else None
            weather_code = parts[1] if len(parts) > 1 else None

        cal[rn] = {
            "race_date":    race_date,
            "track_name":   track,
            "laps":         None,
            "time_of_day":  time_of_day,
            "weather_code": weather_code,
        }
    log.info(f"Streams (S4): {len(cal)} Rennen gefunden.")
    return cal


def parse_calendar_from_db(cur, season_id):
    """Season 3: Kalender aus DB lesen."""
    cur.execute(
        """SELECT r.race_number, r.race_date, r.laps, r.time_of_day, r.weather_code,
                  t.sheet_name as track_name
           FROM races r
           LEFT JOIN tracks t ON r.track_id = t.track_id
           WHERE r.season_id = %s
           ORDER BY r.race_number""",
        (season_id,)
    )
    cal = {}
    for row in cur.fetchall():
        cal[row["race_number"]] = {
            "race_date":    row["race_date"],
            "track_name":   row["track_name"],
            "laps":         row["laps"],
            "time_of_day":  row["time_of_day"],
            "weather_code": row["weather_code"],
        }
    log.info(f"Kalender aus DB: {len(cal)} Rennen fuer Season {season_id}.")
    return cal


# ── Penalties-Parsing ─────────────────────────────────────────────────────────

def parse_penalties(rows):
    """
    Liest Penalties-Sheet.
    Spalte A (idx 0) = Fahrername ab Zeile 6 (idx 5)
    Spalte F (idx 5) = Rennen 1, G=Rennen 2 etc.
    Gibt dict zurueck: {race_number: {psn_name: penalty_seconds}}
    """
    penalties = defaultdict(dict)
    for row in rows[5:]:  # ab Zeile 6 (Index 5)
        psn = cell(row, 0)
        if not psn:
            continue
        for i, col_idx in enumerate(range(5, len(row)), start=1):  # F=5 = Rennen 1
            val = cell(row, col_idx)
            if val and val.isdigit() and int(val) > 0:
                penalties[i][psn] = int(val)
    log.info(f"Penalties geladen: {sum(len(v) for v in penalties.values())} Eintraege.")
    return penalties


# ── Races-Sheet-Parsing ───────────────────────────────────────────────────────

def parse_races_sheet(rows, race_number, season_id, penalties):
    """
    Liest einen Renn-Block aus dem Races-Sheet.

    offset:        Spaltenabstand zwischen Rennen (17 fuer S4/S5, 18 fuer S3)
    race_number:   Welches Rennen (1-basiert)
    Block-Start:   (race_number - 1) * offset + 1  (Spalte B = Index 1 fuer Rennen 1)
    """
    fmt = SEASON_FORMATS[season_id]
    offset     = fmt["offset"]
    boni_cols  = BONI_COLS_S3 if season_id == 3 else BONI_COLS_S45
    col_total  = COL_PTS_TOTAL_S3 if season_id == 3 else COL_PTS_TOTAL_S45

    block_start = (race_number - 1) * offset + 1  # 1-basiert (Spalte B = 1)

    def bc(row, rel_col, default=""):
        """Liest Zelle relativ zum Block-Start."""
        return cell(row, block_start + rel_col, default)

    # FL aus Zeile 4 (Index 3)
    fl_row = rows[FL_ROW] if len(rows) > FL_ROW else []
    fl_driver_psn = cell(fl_row, block_start + FL_COL_DRIVER)
    fl_time_raw   = cell(fl_row, block_start + FL_COL_TIME)

    # Pruefen ob Block Daten enthaelt
    if not fl_driver_psn and not fl_time_raw:
        # Kein FL-Eintrag – pruefen ob ueberhaupt Fahrerdaten vorhanden
        has_data = any(
            cell(row, block_start + COL_DRIVER)
            for row in rows[5:20]
            if len(row) > block_start + COL_DRIVER
        )
        if not has_data:
            return None

    race_penalties = penalties.get(race_number, {})

    entries = []
    for row in rows[5:85]:  # Zeile 6-85 (Index 5-84)
        pos_raw = bc(row, COL_POS)
        if not pos_raw or not pos_raw.isdigit():
            continue

        finish_pos   = int(pos_raw)
        psn_name     = bc(row, COL_DRIVER)
        vehicle_name = bc(row, COL_VEHICLE)
        race_time_raw = bc(row, COL_TIME)
        team_name    = bc(row, COL_TEAM)
        grid_number  = bc(row, COL_GRID)
        pts_base_raw = bc(row, COL_PTS_BASE)
        pts_total_raw = bc(row, col_total)

        if not psn_name:
            continue

        # DNF
        is_dnf = (not race_time_raw
                  or race_time_raw.startswith(DNF_MARKER)
                  or race_time_raw.upper() == "DNF")
        status = "DNF" if is_dnf else "FIN"

        # Zeiten
        race_time_sec = parse_time_to_seconds(race_time_raw) if not is_dnf else None
        penalty_sec   = race_penalties.get(psn_name, 0)

        # race_time_raw ist Zeit MIT Strafe (wie in Legacy)
        # race_time (ohne Strafe) = race_time_final - penalty
        if race_time_sec is not None:
            time_no_penalty = race_time_sec - penalty_sec if penalty_sec else race_time_sec
        else:
            time_no_penalty = None

        # Punkte
        try:
            base_pts = int(pts_base_raw) if pts_base_raw and pts_base_raw.isdigit() else 0
        except ValueError:
            base_pts = 0
        try:
            pts_total = int(pts_total_raw) if pts_total_raw and pts_total_raw.isdigit() else 0
        except ValueError:
            pts_total = 0

        # Boni aus einzelnen Spalten lesen
        # Werte koennen sein: '3', '+3', '-', '' oder leer
        boni = {}
        for bonus_type, rel_col in boni_cols.items():
            val_raw = bc(row, rel_col).strip().lstrip("+")
            try:
                boni[bonus_type] = int(val_raw) if val_raw and val_raw != "-" else 0
            except ValueError:
                boni[bonus_type] = 0

        entries.append({
            "finish_pos":    finish_pos,
            "psn_name":      psn_name,
            "team_name":     team_name,
            "vehicle_name":  vehicle_name,
            "grid_number":   grid_number,
            "penalty_sec":   penalty_sec,
            "penalty_pts":   0,
            "race_time":     seconds_to_timestr(time_no_penalty) if time_no_penalty else None,
            "race_time_final": race_time_raw if not is_dnf else None,
            "time_no_penalty": time_no_penalty,
            "rating":        None,
            "base_points":   base_pts,
            "boni":          boni,
            "points_total":  pts_total,
            "status":        status,
        })

    if not entries:
        return None

    # Siegerzeit und Ratings berechnen
    winner_time = next(
        (e["time_no_penalty"] for e in entries
         if e["finish_pos"] == 1 and e["time_no_penalty"]),
        None
    )
    grid_counters = defaultdict(int)
    for entry in sorted(entries, key=lambda e: e["finish_pos"]):
        if entry["time_no_penalty"] and winner_time and winner_time > 0:
            entry["rating"] = round(entry["time_no_penalty"] / winner_time * 100, 2)
        gn = entry["grid_number"]
        grid_counters[gn] += 1
        entry["finish_pos_grid"] = grid_counters[gn]

    return {
        "fl_driver_psn":    fl_driver_psn,
        "fastest_lap_time": fl_time_raw,
        "entries":          entries,
    }


# ── DB-Import ─────────────────────────────────────────────────────────────────

def import_race(cur, season_id, race_number, data, cal_entry):
    race_id = lookup_or_create_race(cur, season_id, race_number, cal_entry)
    if not race_id:
        return False

    log.info(f"  race_id={race_id} | {len(data['entries'])} Fahrer")

    # FL-Fahrer
    fl_driver_id = None
    if data["fl_driver_psn"]:
        fl_driver_id = lookup_driver(cur, data["fl_driver_psn"])

    cur.execute(
        """UPDATE races SET
             fastest_lap_time      = %s,
             fastest_lap_driver_id = %s
           WHERE race_id = %s""",
        (data["fastest_lap_time"], fl_driver_id, race_id),
    )

    # Alter Stand fuer Change-Detection
    cur.execute(
        "SELECT driver_id, penalty_seconds, time_percent, finish_pos_grid "
        "FROM race_results WHERE race_id = %s",
        (race_id,)
    )
    old_rows  = {r["driver_id"]: r for r in cur.fetchall()}
    old_count = len(old_rows)

    # Bestehende Ergebnisse loeschen
    cur.execute(
        "DELETE FROM bonus_points WHERE result_id IN "
        "(SELECT result_id FROM race_results WHERE race_id = %s)", (race_id,)
    )
    cur.execute("DELETE FROM race_results WHERE race_id = %s", (race_id,))

    # Grids sicherstellen
    grid_numbers = {e["grid_number"] for e in data["entries"] if e["grid_number"]}
    ensure_grids(cur, race_id, grid_numbers)

    inserted          = 0
    new_penalties     = 0
    ratings_updated   = 0
    grid_pos_inserted = 0

    for entry in data["entries"]:
        psn  = entry["psn_name"]
        d_id = lookup_or_create_driver(cur, psn)
        t_id = lookup_team(cur, entry["team_name"])
        v_id = lookup_vehicle(cur, entry["vehicle_name"])
        g_id = lookup_grid(cur, race_id, entry["grid_number"])
        if not g_id:
            log.warning(f"  Grid '{entry['grid_number']}' nicht gefunden (Fahrer: {psn})")

        boni      = entry["boni"]
        bonus_total = sum(boni.values())

        cur.execute(
            """INSERT INTO race_results
               (race_id, grid_id, driver_id, vehicle_id, team_id,
                finish_pos_overall, finish_pos_grid,
                race_time, race_time_final,
                time_percent,
                points_base, bonus_total,
                bonus_fastest_lap, bonus_podium,
                bonus_rare_vehicle, bonus_vehicle_loyalty,
                points_total, status,
                penalty_seconds, penalty_points)
               VALUES
               (%s,%s,%s,%s,%s,
                %s,%s,
                %s,%s,
                %s,
                %s,%s,
                %s,%s,
                %s,%s,
                %s,%s,
                %s,%s)""",
            (
                race_id, g_id, d_id, v_id, t_id,
                entry["finish_pos"],
                entry.get("finish_pos_grid"),
                entry["race_time"],
                entry["race_time_final"],
                entry["rating"],
                entry["base_points"],
                bonus_total,
                boni.get(BONUS_FL, 0),
                boni.get(BONUS_POD, 0),
                boni.get(BONUS_SR, 0),
                boni.get(BONUS_FT, 0),
                entry["points_total"],
                entry["status"],
                entry["penalty_sec"],
                entry["penalty_pts"],
            ),
        )
        result_id = cur.lastrowid

        # bonus_points-Eintraege
        for bonus_type, val in boni.items():
            if val and val > 0:
                cur.execute(
                    "INSERT INTO bonus_points (result_id, bonus_type, points) VALUES (%s,%s,%s)",
                    (result_id, bonus_type, val),
                )

        # Change-Detection
        old = old_rows.get(d_id) or {}
        if entry["penalty_sec"] > 0:
            if (old.get("penalty_seconds") or 0) != entry["penalty_sec"]:
                new_penalties += 1
        if entry["rating"] is not None:
            old_r = old.get("time_percent")
            if old_r is None or abs(float(old_r) - float(entry["rating"])) > 0.01:
                ratings_updated += 1
        if entry.get("finish_pos_grid") and old.get("finish_pos_grid") is None:
            grid_pos_inserted += 1

        inserted += 1

    is_new_race      = old_count == 0
    something_changed = (
        is_new_race or new_penalties > 0 or ratings_updated > 0
        or grid_pos_inserted > 0 or inserted != old_count
    )

    log.info(f"  ✓ {inserted} Fahrer eingetragen."
             + (" (keine Aenderung)" if not something_changed else ""))
    return {
        "new":               is_new_race,
        "changed":           something_changed,
        "drivers":           inserted,
        "penalties":         new_penalties,
        "ratings_updated":   ratings_updated,
        "grid_pos_inserted": grid_pos_inserted,
        "version_updated":   False,
    }


# ── Discord ───────────────────────────────────────────────────────────────────

import urllib.request
import urllib.error


def discord_notify(lines):
    if not DISCORD_TOKEN or not DISCORD_LOG_CH:
        return
    content = "\n".join(lines)
    if len(content) > 1900:
        content = content[:1900] + "\n…"
    url     = f"https://discord.com/api/v10/channels/{DISCORD_LOG_CH}/messages"
    payload = json.dumps({"content": content}).encode("utf-8")
    req     = urllib.request.Request(
        url, data=payload,
        headers={"Authorization": f"Bot {DISCORD_TOKEN}",
                 "Content-Type": "application/json",
                 "User-Agent": "RTC-ImportBot/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status in (200, 201):
                log.info("Discord-Log gepostet.")
    except Exception as e:
        log.warning(f"Discord-Log fehlgeschlagen: {e}")


def post_discord_summary(season_name, changes, errors):
    lines = [f"🗄️ **DB-Update {season_name}**"]
    for rn, r in changes:
        if r.get("new"):
            lines.append(f"  ✅ Rennen {rn} erfasst – {r['drivers']} Fahrer")
        else:
            parts = []
            if r.get("penalties"):
                n = r["penalties"]
                parts.append(f"{n} Strafe{'n' if n != 1 else ''} aktualisiert")
            if r.get("ratings_updated"):
                n = r["ratings_updated"]
                parts.append(f"{n} Rating{'s' if n != 1 else ''} aktualisiert")
            if r.get("grid_pos_inserted"):
                n = r["grid_pos_inserted"]
                parts.append(f"{n} Gridposition{'en' if n != 1 else ''} eingetragen")
            if not parts:
                parts.append("Ergebnisse aktualisiert")
            lines.append(f"  🔄 Rennen {rn} – {', '.join(parts)}")
    if errors:
        lines.append(f"  ⚠️ {errors} Fehler – Details im Log prüfen.")
    discord_notify(lines)


# ── Hauptprogramm ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RTC Sheet Import Seasons 3-5")
    parser.add_argument("--season", type=int, required=True,
                        help="Season-ID (3, 4 oder 5)")
    parser.add_argument("--race", type=int, default=None,
                        help="Nur dieses Rennen importieren")
    args = parser.parse_args()

    if args.season not in SEASON_FORMATS:
        log.error(f"Season {args.season} wird von diesem Script nicht unterstuetzt (nur 3, 4, 5).")
        sys.exit(1)

    db  = get_db()
    cur = db.cursor()

    try:
        cur.execute(
            "SELECT season_id, name, sheet_id FROM seasons WHERE season_id = %s",
            (args.season,)
        )
        season = cur.fetchone()
        if not season:
            log.error(f"Season {args.season} nicht in DB gefunden.")
            sys.exit(1)

        season_id = season["season_id"]
        sheet_id  = season["sheet_id"]

        if not sheet_id:
            log.error(f"Keine sheet_id fuer Season {season_id} hinterlegt.")
            sys.exit(1)

        log.info(f"Saison: {season['name']} (ID={season_id}) | Sheet={sheet_id}")

        svc  = get_sheets_service()
        fmt  = SEASON_FORMATS[season_id]

        # Kalender laden
        cal_type = fmt["calendar"]
        if cal_type == "streams_s5":
            streams_rows = fetch_sheet(svc, sheet_id, "Streams")
            cal = parse_streams_s5(streams_rows)
        elif cal_type == "streams_s4":
            streams_rows = fetch_sheet(svc, sheet_id, "Streams")
            cal = parse_streams_s4(streams_rows)
        else:  # db
            cal = parse_calendar_from_db(cur, season_id)

        # Penalties laden
        penalties_rows = fetch_sheet(svc, sheet_id, "Penaltys")
        penalties = parse_penalties(penalties_rows)

        # Races-Sheet laden
        races_rows = fetch_sheet(svc, sheet_id, "Races")

        # Rennen bestimmen
        if args.race:
            race_numbers = [args.race]
        else:
            race_numbers = sorted(cal.keys())

        log.info(f"Zu importierende Rennen: {race_numbers}")

        imported = 0
        skipped  = 0
        errors   = 0
        changes  = []

        for rn in race_numbers:
            log.info(f"── Rennen {rn} ──────────────")
            cal_entry = cal.get(rn, {})

            data = parse_races_sheet(races_rows, rn, season_id, penalties)
            if not data:
                log.info(f"  Keine Ergebnisse – uebersprungen.")
                skipped += 1
                continue

            result = import_race(cur, season_id, rn, data, cal_entry)
            if result is False:
                errors += 1
            else:
                imported += 1
                if result.get("changed"):
                    changes.append((rn, result))

        db.commit()
        log.info(f"✓ Import abgeschlossen: {imported} importiert, "
                 f"{skipped} uebersprungen, {errors} Fehler.")

        if changes:
            post_discord_summary(season["name"], changes, errors)
        elif errors:
            discord_notify([f"⚠️ DB-Import {season['name']} – {errors} Fehler."])

        if errors:
            sys.exit(1)

    except KeyboardInterrupt:
        db.rollback()
        log.info("Abgebrochen.")
        sys.exit(0)
    except Exception as e:
        db.rollback()
        log.exception(f"Kritischer Fehler: {e}")
        sys.exit(1)
    finally:
        cur.close()
        db.close()


if __name__ == "__main__":
    main()
