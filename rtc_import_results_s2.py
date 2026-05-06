#!/usr/bin/env python3
"""
rtc_import_results_s2.py
────────────────────────
Importiert Rennergebnisse fuer RTC-Saison 2 (season_id=2).

Sheet-Aufbau (ein Blatt "Races", alle Rennen nebeneinander):
  Offset pro Rennen: 10 Spalten
  Block-Start Rennen N: (N-1)*10 + 1  (Spalte B fuer Rennen 1)

  Zeile 2 (idx 1): Block+3 = Streckenname (nicht verwendet, kommt aus DB)
  Zeile 4 (idx 3): Block+2 = FL-Zeit, Block+3 = FL-Driver
  Zeile 6+ (idx 5+): Ergebnisse bis Zeile 53 (idx 52)
    Block+0 = Pos, Block+1 = Grid, Block+2 = RaceTime,
    Block+3 = Driver, Block+4 = Team,
    Block+5 = Punkte gesamt, Block+7 = Punkte Basis, Block+8 = Bonus

  Kein Fahrzeug, keine Strafen, kein separater Kalender (aus DB).
  Bonus wird komplett als bonus_podium eingetragen.

Verwendung:
  python3 rtc_import_results_s2.py
  python3 rtc_import_results_s2.py --race 3
"""

import os
import re
import sys
import json
import logging
import argparse
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime
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

SEASON_ID      = 2
NUM_RACES      = 16
BLOCK_OFFSET   = 10   # Spalten pro Rennen
DATA_START_ROW = 5    # Index 5 = Zeile 6
DATA_END_ROW   = 52   # Index 52 = Zeile 53
FL_ROW         = 3    # Index 3 = Zeile 4

# Relative Spalten-Offsets innerhalb eines Blocks
COL_POS        = 0
COL_GRID       = 1
COL_TIME       = 2
COL_DRIVER     = 3
COL_TEAM       = 4
COL_PTS_TOTAL  = 5
COL_PTS_BASE   = 7
COL_BONUS      = 8
FL_COL_TIME    = 2
FL_COL_DRIVER  = 3

DNF_MARKER = "8:00:00"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "rtc_import_results_s2.log")
        ),
    ],
)
log = logging.getLogger("rtc_import_s2")


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


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def cell(row, idx, default=""):
    try:
        return str(row[idx]).strip()
    except (IndexError, TypeError):
        return default


def parse_time_to_seconds(t):
    if not t:
        return None
    t = t.replace(",", ".")
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


# ── DB-Lookups ────────────────────────────────────────────────────────────────

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
    # Übersetzungen
    TEAM_MAP = {
        "Shiftlock-Racing":       "Shift-Lock-Racing",
        "Maibert Mac Lon Racing": "Maibert MacLon Racing",
        "Pablo Racing Team PRT":  "PRT Competition",
    }
    name = TEAM_MAP.get(name, name)
    cur.execute("SELECT team_id FROM teams WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        return row["team_id"]
    cur.execute("SELECT team_id, name FROM teams WHERE abbreviation = %s LIMIT 1", (name,))
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


def lookup_grid(cur, race_id, grid_number):
    cur.execute(
        "SELECT grid_id FROM grids WHERE race_id = %s AND grid_number = %s",
        (race_id, grid_number),
    )
    row = cur.fetchone()
    return row["grid_id"] if row else None


def ensure_grids(cur, race_id, grid_numbers):
    GRID_LABELS = {
        "1":  ("1", "Grid 1"),
        "2":  ("2", "Grid 2"),
        "2a": ("2", "Grid 2a"),
        "2b": ("2", "Grid 2b"),
        "2c": ("2", "Grid 2c"),
        "3":  ("3", "Grid 3"),
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


def get_race_from_db(cur, season_id, race_number):
    """Liest race_id und Kalenderdaten aus der DB."""
    cur.execute(
        "SELECT race_id, race_date, version_id FROM races "
        "WHERE season_id = %s AND race_number = %s",
        (season_id, race_number),
    )
    return cur.fetchone()


# ── Sheet-Parsing ─────────────────────────────────────────────────────────────

def parse_race_block(rows, race_number):
    """
    Liest einen Renn-Block aus dem Races-Sheet.
    Block-Start = (race_number - 1) * BLOCK_OFFSET + 1
    """
    bs = (race_number - 1) * BLOCK_OFFSET + 1  # block_start

    def bc(row, rel_col, default=""):
        return cell(row, bs + rel_col, default)

    # FL aus Zeile 4 (Index 3)
    fl_row        = rows[FL_ROW] if len(rows) > FL_ROW else []
    fl_time_raw   = cell(fl_row, bs + FL_COL_TIME)
    fl_driver_psn = cell(fl_row, bs + FL_COL_DRIVER)

    entries = []
    for row in rows[DATA_START_ROW:DATA_END_ROW + 1]:
        pos_raw = bc(row, COL_POS)
        if not pos_raw or not pos_raw.isdigit():
            continue

        finish_pos    = int(pos_raw)
        grid_number   = bc(row, COL_GRID)
        race_time_raw = bc(row, COL_TIME)
        psn_name      = bc(row, COL_DRIVER)
        team_name     = bc(row, COL_TEAM)
        pts_total_raw = bc(row, COL_PTS_TOTAL)
        pts_base_raw  = bc(row, COL_PTS_BASE)
        bonus_raw     = bc(row, COL_BONUS)

        if not psn_name:
            continue

        is_dnf = (not race_time_raw
                  or race_time_raw.startswith(DNF_MARKER)
                  or race_time_raw.upper() == "DNF")
        status = "DNF" if is_dnf else "FIN"

        race_time_sec   = parse_time_to_seconds(race_time_raw) if not is_dnf else None
        time_no_penalty = race_time_sec  # keine Strafen in Season 2

        try:
            pts_total = int(pts_total_raw) if pts_total_raw and pts_total_raw.isdigit() else 0
        except ValueError:
            pts_total = 0

        try:
            pts_base = int(pts_base_raw) if pts_base_raw and pts_base_raw.isdigit() else 0
        except ValueError:
            pts_base = 0

        try:
            bonus = int(bonus_raw.lstrip("+")) if bonus_raw and bonus_raw.strip() not in ("", "-") else 0
        except ValueError:
            bonus = 0

        entries.append({
            "finish_pos":      finish_pos,
            "psn_name":        psn_name,
            "team_name":       team_name,
            "grid_number":     grid_number,
            "race_time":       race_time_raw if not is_dnf else None,
            "time_no_penalty": time_no_penalty,
            "rating":          None,
            "base_points":     pts_base,
            "bonus":           bonus,
            "points_total":    pts_total,
            "status":          status,
        })

    if not entries:
        return None

    # Siegerzeit und Ratings
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
        "fl_time_raw":   fl_time_raw,
        "fl_driver_psn": fl_driver_psn,
        "entries":       entries,
    }


# ── DB-Import ─────────────────────────────────────────────────────────────────

def import_race(cur, race_number, data):
    race_row = get_race_from_db(cur, SEASON_ID, race_number)
    if not race_row:
        log.warning(f"  Rennen {race_number} nicht in DB – uebersprungen.")
        return False

    race_id = race_row["race_id"]
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
        (data["fl_time_raw"], fl_driver_id, race_id),
    )

    # Alter Stand
    cur.execute(
        "SELECT driver_id, time_percent, finish_pos_grid "
        "FROM race_results WHERE race_id = %s",
        (race_id,)
    )
    old_rows  = {r["driver_id"]: r for r in cur.fetchall()}
    old_count = len(old_rows)

    # Loeschen
    cur.execute(
        "DELETE FROM bonus_points WHERE result_id IN "
        "(SELECT result_id FROM race_results WHERE race_id = %s)", (race_id,)
    )
    cur.execute("DELETE FROM race_results WHERE race_id = %s", (race_id,))

    # Grids
    grid_numbers = {e["grid_number"] for e in data["entries"] if e["grid_number"]}
    ensure_grids(cur, race_id, grid_numbers)

    inserted          = 0
    ratings_updated   = 0
    grid_pos_inserted = 0

    for entry in data["entries"]:
        psn  = entry["psn_name"]
        d_id = lookup_or_create_driver(cur, psn)
        t_id = lookup_team(cur, entry["team_name"])
        g_id = lookup_grid(cur, race_id, entry["grid_number"])

        if not g_id:
            log.warning(f"  Grid '{entry['grid_number']}' nicht gefunden (Fahrer: {psn})")

        bonus = entry["bonus"]

        cur.execute(
            """INSERT INTO race_results
               (race_id, grid_id, driver_id, team_id,
                finish_pos_overall, finish_pos_grid,
                race_time, race_time_final,
                time_percent,
                points_base, bonus_total, bonus_podium,
                points_total, status,
                penalty_seconds, penalty_points)
               VALUES
               (%s,%s,%s,%s,
                %s,%s,
                %s,%s,
                %s,
                %s,%s,%s,
                %s,%s,
                %s,%s)""",
            (
                race_id, g_id, d_id, t_id,
                entry["finish_pos"],
                entry.get("finish_pos_grid"),
                entry["race_time"],
                entry["race_time"],   # race_time_final = race_time (keine Strafe)
                entry["rating"],
                entry["base_points"],
                bonus,
                bonus,                # bonus_podium = gesamter Bonus
                entry["points_total"],
                entry["status"],
                0, 0,
            ),
        )
        result_id = cur.lastrowid

        # bonus_points-Eintrag wenn Bonus vorhanden
        if bonus > 0:
            cur.execute(
                "INSERT INTO bonus_points (result_id, bonus_type, points) VALUES (%s,%s,%s)",
                (result_id, "POD", bonus),
            )

        # Change-Detection
        old = old_rows.get(d_id) or {}
        if entry["rating"] is not None:
            old_r = old.get("time_percent")
            if old_r is None or abs(float(old_r) - float(entry["rating"])) > 0.01:
                ratings_updated += 1
        if entry.get("finish_pos_grid") and old.get("finish_pos_grid") is None:
            grid_pos_inserted += 1

        inserted += 1

    is_new       = old_count == 0
    changed      = is_new or ratings_updated > 0 or grid_pos_inserted > 0 or inserted != old_count

    log.info(f"  ✓ {inserted} Fahrer eingetragen."
             + (" (keine Aenderung)" if not changed else ""))
    return {
        "new":               is_new,
        "changed":           changed,
        "drivers":           inserted,
        "penalties":         0,
        "ratings_updated":   ratings_updated,
        "grid_pos_inserted": grid_pos_inserted,
        "version_updated":   False,
    }


# ── Discord ───────────────────────────────────────────────────────────────────

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
    parser = argparse.ArgumentParser(description="RTC Sheet Import Season 2")
    parser.add_argument("--race", type=int, default=None,
                        help="Nur dieses Rennen importieren (1-16)")
    args = parser.parse_args()

    db  = get_db()
    cur = db.cursor()

    try:
        cur.execute(
            "SELECT season_id, name, sheet_id FROM seasons WHERE season_id = %s",
            (SEASON_ID,)
        )
        season = cur.fetchone()
        if not season:
            log.error(f"Season {SEASON_ID} nicht in DB gefunden.")
            sys.exit(1)

        sheet_id = season["sheet_id"]
        if not sheet_id:
            log.error(f"Keine sheet_id fuer Season {SEASON_ID} hinterlegt.")
            sys.exit(1)

        log.info(f"Saison: {season['name']} (ID={SEASON_ID}) | Sheet={sheet_id}")

        svc       = get_sheets_service()
        races_rows = fetch_sheet(svc, sheet_id, "Gesamt-Ergebnisse")

        race_numbers = [args.race] if args.race else list(range(1, NUM_RACES + 1))
        log.info(f"Zu importierende Rennen: {race_numbers}")

        imported = 0
        skipped  = 0
        errors   = 0
        changes  = []

        for rn in race_numbers:
            log.info(f"── Rennen {rn} ──────────────")
            data = parse_race_block(races_rows, rn)

            if not data:
                log.info(f"  Keine Ergebnisse – uebersprungen.")
                skipped += 1
                continue

            result = import_race(cur, rn, data)
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
