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
    Liest alle Rennen aus dem Übersicht-Sheet.
    Erkennt Rennen-Start wenn Spalte C eine Zahl ist und Spalte B 'Race' oder leer.
    Gibt dict zurueck: {race_number: {entries, fl_driver, fl_time}}
    """
    races = {}
    i = 0
    while i < len(rows):
        row = rows[i]
        # Rennen-Header erkennen: Spalte C ist Rennnummer
        b = cell(row, 1)
        c = cell(row, 2)

        if c.isdigit() and (b in ("Race", "") or b == "Race"):
            race_number = int(c)
            i += 1  # Streckenname-Zeile
            i += 1  # Header-Zeile (POS, NAME...)
            i += 1  # Leerzeile

            entries    = []
            fl_driver  = None
            fl_time    = None
            finish_pos = 0

            while i < len(rows):
                r = rows[i]
                d = cell(r, 3)  # POS oder DNF/DNS oder 'Fastest LAP'
                e = cell(r, 4)  # NAME

                # Fastest LAP Block
                if d == "" and e == "" and cell(r, 4) == "" and i + 2 < len(rows):
                    # Prüfen ob zwei Zeilen später FL-Daten stehen
                    fl_row = rows[i + 2] if i + 2 < len(rows) else []
                    fl_name = cell(fl_row, 4)
                    fl_t    = cell(fl_row, 7)
                    if fl_name or fl_t:
                        fl_driver = fl_name
                        fl_time   = fl_t
                    i += 5  # FL-Block überspringen (5 Zeilen)
                    break

                # Nächstes Rennen
                if cell(r, 2).isdigit() and cell(r, 1) in ("Race", ""):
                    break

                # Fahrereintrag
                if d.isdigit():
                    finish_pos = int(d)
                    status     = "FIN"
                elif d in ("DNF", "DNS"):
                    finish_pos += 1  # DNF/DNS werden nach finishers gezählt
                    status = d
                else:
                    i += 1
                    continue

                psn_name  = e
                time_raw  = cell(r, 5)
                pts_raw   = cell(r, 6)
                grid_raw  = cell(r, 7)
                team_name = cell(r, 8)

                if not psn_name:
                    i += 1
                    continue

                # DNS: Zeit ist "DNS", Grid ist "x"
                if status == "DNS":
                    time_raw = None
                    grid_raw = None

                time_sec = parse_time_to_seconds(time_raw) if time_raw else None

                try:
                    pts = int(pts_raw) if pts_raw and pts_raw.isdigit() else 0
                except ValueError:
                    pts = 0

                entries.append({
                    "finish_pos":  finish_pos if status == "FIN" else None,
                    "psn_name":    psn_name,
                    "team_name":   team_name,
                    "grid_number": grid_raw if grid_raw and grid_raw != "x" else None,
                    "race_time":   time_raw if status == "FIN" else None,
                    "time_sec":    time_sec,
                    "points_total": pts,
                    "status":      status,
                    "rating":      None,
                })
                i += 1
                continue

            # Siegerzeit und Ratings berechnen
            winner_time = next(
                (e["time_sec"] for e in entries
                 if e["status"] == "FIN" and e["time_sec"]),
                None
            )
            # finish_pos_grid berechnen
            grid_counters = defaultdict(int)
            finishers = sorted(
                [e for e in entries if e["status"] == "FIN" and e["finish_pos"]],
                key=lambda x: x["finish_pos"]
            )
            for entry in finishers:
                gn = entry["grid_number"] or "?"
                grid_counters[gn] += 1
                entry["finish_pos_grid"] = grid_counters[gn]
                if entry["time_sec"] and winner_time and winner_time > 0:
                    entry["rating"] = round(entry["time_sec"] / winner_time * 100, 2)

            # DNF/DNS ohne finish_pos: nach den Finishern anhängen
            non_finishers = [e for e in entries if e["status"] != "FIN"]
            for j, entry in enumerate(non_finishers):
                entry["finish_pos"] = len(finishers) + j + 1
                entry["finish_pos_grid"] = None

            races[race_number] = {
                "entries":   entries,
                "fl_driver": fl_driver,
                "fl_time":   fl_time,
            }
            log.info(f"  Rennen {race_number}: {len(entries)} Eintraege geparst, "
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
        g_id = lookup_grid(cur, race_id, entry["grid_number"]) if entry["grid_number"] else None

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
