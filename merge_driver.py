#!/usr/bin/env python3
"""
merge_driver.py
───────────────
Zusammenfuehren zweier Fahrer-Eintraege in der Datenbank.

Verwendung:
  python3 merge_driver.py <keep_id> <merge_id>

  keep_id:  Fahrer-ID die behalten wird (Ziel)
  merge_id: Fahrer-ID die zusammengefuehrt und geloescht wird (Quelle)

Was passiert:
  1. Alle race_results von merge_id werden auf keep_id umgeschrieben
  2. Alle team_memberships von merge_id werden auf keep_id umgeschrieben
       (Duplikate werden uebersprungen)
  3. psn_name von merge_id wird in name_history von keep_id eingetragen
  4. merge_id wird aus der drivers-Tabelle geloescht

Beispiel:
  python3 merge_driver.py 42 137
  → Fahrer 137 wird in Fahrer 42 gemergt, 137 wird geloescht
"""

import sys
import json
import logging
from dotenv import load_dotenv
import os

import pymysql

# ── Konfiguration ─────────────────────────────────────────────────────────────

ENV_PATH = "/etc/RTC_RaceResultBot-env"
load_dotenv(ENV_PATH)

DB_HOST     = os.getenv("DB_HOST")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME     = os.getenv("DB_NAME")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("merge_driver")


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


def main():
    if len(sys.argv) != 3:
        print("Verwendung: python3 merge_driver.py <keep_id> <merge_id>")
        sys.exit(1)

    try:
        keep_id  = int(sys.argv[1])
        merge_id = int(sys.argv[2])
    except ValueError:
        print("Fehler: IDs muessen Ganzzahlen sein.")
        sys.exit(1)

    if keep_id == merge_id:
        print("Fehler: keep_id und merge_id sind identisch.")
        sys.exit(1)

    db  = get_db()
    cur = db.cursor()

    try:
        # ── Beide Fahrer laden ────────────────────────────────────────────────
        cur.execute("SELECT * FROM drivers WHERE driver_id = %s", (keep_id,))
        keep = cur.fetchone()
        if not keep:
            log.error(f"Fahrer mit ID {keep_id} nicht gefunden.")
            sys.exit(1)

        cur.execute("SELECT * FROM drivers WHERE driver_id = %s", (merge_id,))
        merge = cur.fetchone()
        if not merge:
            log.error(f"Fahrer mit ID {merge_id} nicht gefunden.")
            sys.exit(1)

        log.info(f"Behalte:  [{keep_id}] {keep['psn_name']}")
        log.info(f"Merge:    [{merge_id}] {merge['psn_name']}")
        print()

        # ── Vorschau ─────────────────────────────────────────────────────────
        cur.execute(
            "SELECT COUNT(*) as n FROM race_results WHERE driver_id = %s", (merge_id,)
        )
        n_results = cur.fetchone()["n"]

        cur.execute(
            "SELECT COUNT(*) as n FROM team_memberships WHERE driver_id = %s", (merge_id,)
        )
        n_memberships = cur.fetchone()["n"]

        log.info(f"Zu uebertragen: {n_results} Rennergebnisse, {n_memberships} Team-Mitgliedschaften")

        # Bestehende name_history von keep_id
        existing_history = []
        if keep["name_history"]:
            try:
                existing_history = json.loads(keep["name_history"])
                if not isinstance(existing_history, list):
                    existing_history = [str(existing_history)]
            except (json.JSONDecodeError, TypeError):
                existing_history = [keep["name_history"]]

        merge_name = merge["psn_name"]
        if merge_name not in existing_history:
            new_history = existing_history + [merge_name]
        else:
            new_history = existing_history
            log.info(f"  '{merge_name}' ist bereits in name_history – wird nicht doppelt eingetragen.")

        log.info(f"name_history nach Merge: {new_history}")
        print()

        # ── Bestaetigung ─────────────────────────────────────────────────────
        confirm = input(f"Fortfahren? Fahrer {merge_id} ({merge['psn_name']}) wird "
                        f"in {keep_id} ({keep['psn_name']}) gemergt und geloescht. [j/N] ")
        if confirm.strip().lower() != "j":
            log.info("Abgebrochen.")
            sys.exit(0)

        # ── race_results umschreiben ──────────────────────────────────────────
        cur.execute(
            "UPDATE race_results SET driver_id = %s WHERE driver_id = %s",
            (keep_id, merge_id),
        )
        log.info(f"  {cur.rowcount} Rennergebnisse auf Fahrer {keep_id} umgeschrieben.")

        # ── team_memberships umschreiben ──────────────────────────────────────
        if n_memberships > 0:
            # Duplikate vermeiden: erst pruefen welche schon existieren
            cur.execute(
                "SELECT team_id, season_id FROM team_memberships WHERE driver_id = %s",
                (keep_id,)
            )
            existing_memberships = {(r["team_id"], r["season_id"]) for r in cur.fetchall()}

            cur.execute(
                "SELECT team_id, season_id FROM team_memberships WHERE driver_id = %s",
                (merge_id,)
            )
            merge_memberships = cur.fetchall()

            transferred = 0
            skipped     = 0
            for m in merge_memberships:
                key = (m["team_id"], m["season_id"])
                if key in existing_memberships:
                    log.info(f"  Mitgliedschaft team_id={m['team_id']} season_id={m['season_id']} "
                             f"existiert bereits – uebersprungen.")
                    skipped += 1
                else:
                    cur.execute(
                        "UPDATE team_memberships SET driver_id = %s "
                        "WHERE driver_id = %s AND team_id = %s AND season_id = %s",
                        (keep_id, merge_id, m["team_id"], m["season_id"]),
                    )
                    transferred += 1

            # Verbleibende Duplikate loeschen
            cur.execute(
                "DELETE FROM team_memberships WHERE driver_id = %s", (merge_id,)
            )
            log.info(f"  {transferred} Mitgliedschaften uebertragen, {skipped} Duplikate uebersprungen.")

        # ── name_history aktualisieren ────────────────────────────────────────
        cur.execute(
            "UPDATE drivers SET name_history = %s WHERE driver_id = %s",
            (json.dumps(new_history, ensure_ascii=False), keep_id),
        )
        log.info(f"  name_history aktualisiert: {new_history}")

        # ── fastest_lap_driver_id in races umschreiben ───────────────────────
        cur.execute(
            "UPDATE races SET fastest_lap_driver_id = %s WHERE fastest_lap_driver_id = %s",
            (keep_id, merge_id),
        )
        if cur.rowcount:
            log.info(f"  {cur.rowcount} Rennen mit fastest_lap_driver_id aktualisiert.")

        # ── Fahrer loeschen ───────────────────────────────────────────────────
        cur.execute("DELETE FROM drivers WHERE driver_id = %s", (merge_id,))
        log.info(f"  Fahrer {merge_id} ({merge_name}) geloescht.")

        db.commit()
        log.info(f"✓ Merge abgeschlossen: [{merge_id}] {merge_name} → [{keep_id}] {keep['psn_name']}")

    except KeyboardInterrupt:
        db.rollback()
        log.info("Abgebrochen.")
        sys.exit(0)
    except Exception as e:
        db.rollback()
        log.exception(f"Fehler: {e}")
        sys.exit(1)
    finally:
        cur.close()
        db.close()


if __name__ == "__main__":
    main()
