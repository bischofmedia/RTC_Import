#!/usr/bin/env python3
"""
rtc_import_results_s1.py
────────────────────────
Importiert Rennergebnisse fuer RTC-Saison 1 (season_id=1).

Sheet-Aufbau (Tab "Übersicht", alle Rennen untereinander):
  Jedes Rennen beginnt mit einer Zeile wo Spalte B = 'Race' oder leer,
  Spalte C = Rennnummer.
  Darunter: Streckenname in Spalte B.
  Dann Header-Zeile (POS, NAME, TIME...).
  Dann Ergebnisse: D=POS, E=NAME, F=TIME, G=PKT, H=GRID, I=TEAM
  DNF/DNS werden als Status eingetragen.
  Nach den Ergebnissen: FL-Block mit NAME in E, ZEIT in H.

  Kein Fahrzeug, keine Strafen, keine Boni.
  Datum aus DB (races-Tabelle).

Verwendung:
  python3 rtc_import_results_s1.py
  python3 rtc_import_results_s1.py --race 3
"""

import os
import sys
import json
import logging
import argparse
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime

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

SEASON_ID  = 1
SHEET_TAB  = "Übersicht"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "rtc_import_results_s1.log")
        ),
    ],
)
log = logging.getLogger("rtc_import_s1")


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
    # Bekannte Abweichungen
    TEAM_MAP = {
        "Shiftlock-Racing":       "Shift-Lock-Racing",
        "Maibert Mac Lon Racing": "Maibert MacLon Racing",
        "Coyote Racing":          "Racing Team Coyote",
        "No Facksen Racing":      "No Facksen Racing",
        "No Facksen Racing 2":    "No Facksen Racing",
        "Beta GTS Racing":        "Beta GTS Racing",
        "Roadrunner Racing":      "Roadrunner Racing",
        "Madson Racing":          "Madson Racing",
        "Benny GP":               "Benny GP",
        "Gaskarl-Racing":         "Gaskarl-Racing",
        "Hellboy Racing AC":      "Hellboy Racing AC",
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


def lookup_or_ensure_grid(cur, race_id, grid_number):
    """Sucht grid_id, legt Grid an falls nicht vorhanden."""
    if not grid_number:
        return None
    cur.execute(
        "SELECT grid_id FROM grids WHERE race_id = %s AND grid_number = %s",
        (race_id, grid_number),
    )
    row = cur.fetchone()
    if row:
        return row["grid_id"]
    # Anlegen
    GRID_LABELS = {
        "1": ("1", "Grid 1"), "2": ("2", "Grid 2"),
        "2a": ("2", "Grid 2a"), "2b": ("2", "Grid 2b"), "3": ("3", "Grid 3"),
    }
    grid_class, grid_label = GRID_LABELS.get(grid_number, (grid_number, f"Grid {grid_number}"))
    cur.execute(
        "INSERT INTO grids (race_id, grid_number, grid_class, grid_label) VALUES (%s,%s,%s,%s)",
        (race_id, grid_number, grid_class, grid_label),
    )
    log.info(f"  Grid '{grid_number}' fuer race_id={race_id} angelegt.")
    return cur.lastrowid


def ensure_grids(cur, race_id, grid_numbers):
    GRID_LABELS = {
        "1":  ("1", "Grid 1"),
        "2":  ("2", "Grid 2"),
        "2a": ("2", "Grid 2a"),
        "2b": ("2", "Grid 2b"),
        "3":  ("3", "Grid 3"),
        "x":  ("x", "DNS"),
    }
    for gn in sorted(grid_numbers):
        if gn in ("x", ""):
            continue
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


def get_race_from_db(cur, season_id, race_number):
    cur.execute(
        "SELECT race_id, race_date FROM races "
        "WHERE season_id = %s AND race_number = %s",
        (season_id, race_number),
    )
    return cur.fetchone()


# ── Sheet-Parsing ─────────────────────────────────────────────────────────────

def parse_all_races(rows):
    """
    Liest alle Rennen aus dem Uebersicht-Sheet.

    Struktur:
      Rennen-Header: B (idx 1) = 'Race', C (idx 2) = Rennnummer
      Strecke:       naechste Zeile, B (idx 1)
      Header:        POS=D(3), NAME=E(4), TIME=F(5), PKT=G(6), GRID=H(7), TEAM=I(8)
      Daten:         D=POS/DNF/DNS, E=NAME, F=TIME, G=PKT, H=GRID, I=TEAM
      FL-Block:      F (idx 5) = 'Fastest LAP', dann 2 Zeilen darunter NAME in E(4), ZEIT in H(7)
    """
    races = {}
    i = 0

    while i < len(rows):
        row = rows[i]
        # Rennen-Header: B='Race', C=Zahl
        if cell(row, 1) == "Race" and cell(row, 2).isdigit():
            race_number = int(cell(row, 2))
            i += 1  # Streckenname
            i += 1  # Header-Zeile
            i += 1  # Leerzeile
            i += 1  # Erste Datenzeile

            entries             = []
            fl_driver           = None
            fl_time             = None
            finish_pos_counter  = 0

            while i < len(rows):
                r    = rows[i]
                pos  = cell(r, 3)   # D: POS / DNF / DNS
                name = cell(r, 4)   # E: NAME

                # FL-Block erkennen: F (idx 5) = 'Fastest LAP'
                if cell(r, 5) == "Fastest LAP":
                    if i + 2 < len(rows):
                        fl_row    = rows[i + 2]
                        fl_driver = cell(fl_row, 4)  # E
                        fl_time   = cell(fl_row, 7)  # H
                    i += 5
                    break

                # Naechstes Rennen
                if cell(r, 1) == "Race" and cell(r, 2).isdigit():
                    break

                # Fahrereintrag
                if pos.isdigit() or pos in ("DNF", "DNS"):
                    if not name:
                        i += 1
                        continue

                    status = pos if pos in ("DNF", "DNS") else "FIN"

                    if status == "FIN":
                        finish_pos_counter = int(pos)
                    else:
                        finish_pos_counter += 1

                    finish_pos = finish_pos_counter
                    time_raw   = cell(r, 5)  # F
                    pts_raw    = cell(r, 6)  # G
                    grid_raw   = cell(r, 7)  # H
                    team_name  = cell(r, 8)  # I

                    if status == "DNS":
                        time_raw = None
                        grid_raw = None

                    time_sec = parse_time_to_seconds(time_raw) if time_raw else None

                    try:
                        pts = int(pts_raw) if pts_raw and pts_raw.isdigit() else 0
                    except ValueError:
                        pts = 0

                    entries.append({
                        "finish_pos":      finish_pos,
                        "psn_name":        name,
                        "team_name":       team_name,
                        "grid_number":     grid_raw if grid_raw and grid_raw != "x" else None,
                        "race_time":       time_raw if status == "FIN" else None,
                        "time_sec":        time_sec,
                        "points_total":    pts,
                        "status":          status,
                        "rating":          None,
                        "finish_pos_grid": None,
                    })

                i += 1

            # Siegerzeit, Ratings und finish_pos_grid berechnen
            winner_time = next(
                (e["time_sec"] for e in entries
                 if e["status"] == "FIN" and e["time_sec"]),
                None
            )
            grid_counters = defaultdict(int)
            for entry in sorted(
                [e for e in entries if e["status"] == "FIN"],
                key=lambda x: x["finish_pos"]
            ):
                gn = entry["grid_number"] or "?"
                grid_counters[gn] += 1
                entry["finish_pos_grid"] = grid_counters[gn]
                if entry["time_sec"] and winner_time and winner_time > 0:
                    entry["rating"] = round(entry["time_sec"] / winner_time * 100, 2)

            races[race_number] = {
                "entries":   entries,
                "fl_driver": fl_driver,
                "fl_time":   fl_time,
            }
            log.info(f"  Rennen {race_number}: {len(entries)} Eintraege, "
                     f"FL: {fl_driver} ({fl_time})")
        else:
            i += 1

    return races



# ── DB-Import ─────────────────────────────────────────────────────────────────

def import_race(cur, race_number, data):
    race_row = get_race_from_db(cur, SEASON_ID, race_number)
    if not race_row:
        log.warning(f"  Rennen {race_number} nicht in DB – uebersprungen.")
        return False

    race_id = race_row["race_id"]
    entries = data["entries"]
    log.info(f"  race_id={race_id} | {len(entries)} Eintraege")

    # FL-Fahrer
    fl_driver_id = None
    if data["fl_driver"]:
        fl_driver_id = lookup_driver(cur, data["fl_driver"])

    cur.execute(
        "UPDATE races SET fastest_lap_time = %s, fastest_lap_driver_id = %s "
        "WHERE race_id = %s",
        (data["fl_time"], fl_driver_id, race_id),
    )

    # Alter Stand
    cur.execute(
        "SELECT driver_id, time_percent, finish_pos_grid FROM race_results "
        "WHERE race_id = %s", (race_id,)
    )
    old_rows  = {r["driver_id"]: r for r in cur.fetchall()}
    old_count = len(old_rows)

    # Loeschen
    cur.execute(
        "DELETE FROM bonus_points WHERE result_id IN "
        "(SELECT result_id FROM race_results WHERE race_id = %s)", (race_id,)
    )
    cur.execute("DELETE FROM race_results WHERE race_id = %s", (race_id,))

    # Grids sicherstellen
    grid_numbers = {e["grid_number"] for e in entries if e.get("grid_number")}
    ensure_grids(cur, race_id, grid_numbers)

    inserted          = 0
    ratings_updated   = 0
    grid_pos_inserted = 0

    for entry in entries:
        psn  = entry["psn_name"]
        d_id = lookup_or_create_driver(cur, psn)
        t_id = lookup_team(cur, entry["team_name"])
        g_id = lookup_or_ensure_grid(cur, race_id, entry["grid_number"]) if entry["grid_number"] else None
        if not g_id and entry["grid_number"]:
            log.warning(f"  Grid '{entry['grid_number']}' nicht gefunden fuer {psn}")

        cur.execute(
            """INSERT INTO race_results
               (race_id, grid_id, driver_id, team_id,
                finish_pos_overall, finish_pos_grid,
                race_time, race_time_final,
                time_percent,
                points_base, bonus_total, points_total,
                status, penalty_seconds, penalty_points)
               VALUES
               (%s,%s,%s,%s,
                %s,%s,
                %s,%s,
                %s,
                %s,%s,%s,
                %s,%s,%s)""",
            (
                race_id, g_id, d_id, t_id,
                entry["finish_pos"],
                entry.get("finish_pos_grid"),
                entry["race_time"],
                entry["race_time"],
                entry["rating"],
                entry["points_total"],
                0,
                entry["points_total"],
                entry["status"],
                0, 0,
            ),
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

    is_new  = old_count == 0
    changed = is_new or ratings_updated > 0 or grid_pos_inserted > 0 or inserted != old_count

    log.info(f"  ✓ {inserted} Fahrer eingetragen."
             + (" (keine Aenderung)" if not changed else ""))
    return {
        "new":               is_new,
        "changed":           changed,
        "drivers":           inserted,
        "ratings_updated":   ratings_updated,
        "grid_pos_inserted": grid_pos_inserted,
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
    parser = argparse.ArgumentParser(description="RTC Sheet Import Season 1")
    parser.add_argument("--race", type=int, default=None,
                        help="Nur dieses Rennen importieren (1-24)")
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
            log.error(f"Season {SEASON_ID} nicht in DB.")
            sys.exit(1)

        sheet_id = season["sheet_id"]
        if not sheet_id:
            log.error(f"Keine sheet_id fuer Season {SEASON_ID} hinterlegt.")
            sys.exit(1)

        log.info(f"Saison: {season['name']} (ID={SEASON_ID}) | Sheet={sheet_id}")

        svc  = get_sheets_service()
        rows = fetch_sheet(svc, sheet_id, SHEET_TAB)

        log.info(f"Sheet geladen: {len(rows)} Zeilen")
        all_races = parse_all_races(rows)
        log.info(f"Gefundene Rennen im Sheet: {sorted(all_races.keys())}")

        if args.race:
            race_numbers = [args.race]
        else:
            race_numbers = sorted(all_races.keys())

        imported = 0
        skipped  = 0
        errors   = 0
        changes  = []

        for rn in race_numbers:
            log.info(f"── Rennen {rn} ──────────────")
            if rn not in all_races:
                log.warning(f"  Rennen {rn} nicht im Sheet gefunden.")
                skipped += 1
                continue

            result = import_race(cur, rn, all_races[rn])
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
