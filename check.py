#!/usr/bin/env python3
"""
RTC Season Check Script
Prüft welche Rennen einer Season in der DB vorhanden sind

Verwendung:
    python3 check.py 12
    python3 check.py 12 --details
"""

import sys
import os
import mysql.connector
from mysql.connector import Error

def connect_db():
    try:
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '3306')),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except Error as e:
        print(f"✗ DB-Fehler: {e}")
        sys.exit(1)

def check_season(season_id, show_details=False):
    conn = connect_db()
    cursor = conn.cursor()

    print("="*70)
    print(f"Season {season_id} - Status Check")
    print("="*70)

    cursor.execute("SELECT name, start_date, end_date FROM seasons WHERE season_id = %s", (season_id,))
    season_info = cursor.fetchone()
    if not season_info:
        print(f"✗ Season {season_id} nicht in DB gefunden!")
        cursor.close(); conn.close(); return

    season_name, start_date, end_date = season_info
    print(f"\nSeason: {season_name}")
    print(f"Zeitraum: {start_date} bis {end_date}")

    cursor.execute("SELECT COUNT(*) FROM races WHERE season_id = %s", (season_id,))
    race_count = cursor.fetchone()[0]

    print(f"\n📊 Statistik:")
    print(f"  Rennen in DB: {race_count}")

    if race_count == 0:
        print(f"\n✗ Keine Rennen für Season {season_id} gefunden!")
        cursor.close(); conn.close(); return

    cursor.execute("""
        SELECT r.race_id, r.race_number, r.race_date, t.name, t.variant,
               COUNT(rr.result_id) as result_count
        FROM races r
        LEFT JOIN tracks t ON r.track_id = t.track_id
        LEFT JOIN race_results rr ON r.race_id = rr.race_id
        WHERE r.season_id = %s
        GROUP BY r.race_id
        ORDER BY r.race_number
    """, (season_id,))
    races = cursor.fetchall()

    total_results = sum(r[5] for r in races)
    avg_results = total_results / len(races) if races else 0
    print(f"  Gesamt-Ergebnisse: {total_results}")
    print(f"  Ø Fahrer pro Rennen: {avg_results:.1f}")

    race_numbers = sorted([r[1] for r in races if r[1]])
    if race_numbers:
        expected = list(range(1, max(race_numbers) + 1))
        missing = [n for n in expected if n not in race_numbers]
        if missing:
            print(f"\n⚠️  Fehlende Rennen-Nummern: {missing}")
        else:
            print(f"\n✓ Alle Rennen 1-{max(race_numbers)} vorhanden")

    if show_details:
        print("\n" + "="*70)
        print("Rennen-Details:")
        print("="*70)
        print(f"{'Nr':<4} {'Race ID':<8} {'Datum':<12} {'Track':<35} {'Fahrer':<7}")
        print("-"*70)
        for race in races:
            race_id, race_num, race_date, track_name, variant, result_count = race
            track_full = f"{track_name} {variant}" if variant else track_name
            print(f"{str(race_num):<4} {race_id:<8} {race_date} {track_full:<35} {result_count:<7}")

    # Grids
    cursor.execute("""
        SELECT COUNT(DISTINCT g.grid_id)
        FROM grids g JOIN races r ON g.race_id = r.race_id
        WHERE r.season_id = %s
    """, (season_id,))
    grid_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT g.grid_number, g.grid_class, COUNT(DISTINCT g.grid_id) as count
        FROM grids g JOIN races r ON g.race_id = r.race_id
        WHERE r.season_id = %s
        GROUP BY g.grid_number, g.grid_class
        ORDER BY g.grid_number
    """, (season_id,))
    grid_dist = cursor.fetchall()

    print(f"\n📋 Grids:")
    print(f"  Gesamt: {grid_count}")
    if grid_dist:
        print(f"  Verteilung:")
        for gnum, gcls, count in grid_dist:
            print(f"    Grid {gnum} ({gcls}): {count}x")

    # Fahrer pro Grid
    cursor.execute("""
        SELECT sub.grid_number, MIN(sub.cnt) as min_f, MAX(sub.cnt) as max_f
        FROM (
            SELECT g.grid_number, g.grid_id, COUNT(*) as cnt
            FROM race_results rr
            JOIN grids g ON rr.grid_id = g.grid_id
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.season_id = %s
            GROUP BY g.grid_id, g.grid_number
        ) sub
        GROUP BY sub.grid_number
        ORDER BY sub.grid_number
    """, (season_id,))
    grid_sizes = cursor.fetchall()
    if grid_sizes:
        print(f"  Fahrer pro Grid (min-max):")
        for gnum, min_f, max_f in grid_sizes:
            print(f"    Grid {gnum}: {min_f}-{max_f} Fahrer")

    # Teilnehmer
    cursor.execute("""
        SELECT COUNT(DISTINCT driver_id) FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id WHERE r.season_id = %s
    """, (season_id,))
    driver_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(DISTINCT team_id) FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND team_id IS NOT NULL
    """, (season_id,))
    team_count = cursor.fetchone()[0]

    print(f"\n👥 Teilnehmer:")
    print(f"  Fahrer: {driver_count}")
    print(f"  Teams: {team_count}")

    # DNFs
    cursor.execute("""
        SELECT COUNT(*) FROM race_results rr JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.status = 'DNF'
    """, (season_id,))
    dnf_count = cursor.fetchone()[0]
    if dnf_count > 0:
        print(f"\n🔧 DNFs: {dnf_count}")

    # Strafen
    cursor.execute("""
        SELECT COUNT(*) FROM race_results rr JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.penalty_seconds > 0
    """, (season_id,))
    penalty_sec_count = cursor.fetchone()[0]

    # penalty_points Spalte prüfen
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM race_results rr JOIN races r ON rr.race_id = r.race_id
            WHERE r.season_id = %s AND rr.penalty_points > 0
        """, (season_id,))
        penalty_pts_count = cursor.fetchone()[0]
    except Exception:
        penalty_pts_count = None

    print(f"\n⏱️  Strafen:")
    print(f"  penalty_seconds:  {'✓ ' + str(penalty_sec_count) + ' Einträge' if penalty_sec_count else '⚠️  keine Daten'}")
    if penalty_pts_count is None:
        print(f"  penalty_points:   ⚠️  Spalte nicht vorhanden")
    else:
        print(f"  penalty_points:   {'✓ ' + str(penalty_pts_count) + ' Einträge' if penalty_pts_count else '⚠️  keine Daten'}")

    # Bonuspunkte
    cursor.execute("""
        SELECT
            SUM(CASE WHEN bonus_total > 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN bonus_podium > 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN bonus_fastest_lap > 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN bonus_rare_vehicle > 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN bonus_vehicle_loyalty > 0 THEN 1 ELSE 0 END)
        FROM race_results rr JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s
    """, (season_id,))
    bonus = cursor.fetchone()

    print(f"\n🏆 Bonuspunkte:")
    print(f"  bonus_total:           {'✓ ' + str(bonus[0]) + ' Einträge' if bonus[0] else '⚠️  keine Daten'}")
    print(f"  bonus_podium:          {'✓ ' + str(bonus[1]) + ' Einträge' if bonus[1] else '⚠️  keine Daten'}")
    print(f"  bonus_fastest_lap:     {'✓ ' + str(bonus[2]) + ' Einträge' if bonus[2] else '⚠️  keine Daten'}")
    print(f"  bonus_rare_vehicle:    {'✓ ' + str(bonus[3]) + ' Einträge' if bonus[3] else '⚠️  keine Daten'}")
    print(f"  bonus_vehicle_loyalty: {'✓ ' + str(bonus[4]) + ' Einträge' if bonus[4] else '⚠️  keine Daten'}")


    # Plausibilitätsprüfung: points_base + alle Boni = points_total
    cursor.execute("""
        SELECT d.psn_name, r.race_number,
               rr.points_base, rr.bonus_podium, rr.bonus_fastest_lap,
               rr.bonus_rare_vehicle, rr.bonus_vehicle_loyalty, rr.points_total
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        JOIN drivers d ON rr.driver_id = d.driver_id
        WHERE r.season_id = %s
        AND (rr.points_base + rr.bonus_podium + rr.bonus_fastest_lap +
             rr.bonus_rare_vehicle + rr.bonus_vehicle_loyalty) != rr.points_total
        AND rr.points_total > 0
        LIMIT 10
    """, (season_id,))
    plausibility_errors = cursor.fetchall()

    if plausibility_errors:
        print(f"\n  \u26a0\ufe0f  Plausibilitätsfehler (basis+boni \u2260 gesamt):")
        for driver, race_num, base, pod, fl, rare, loy, ptotal in plausibility_errors:
            calc = base + pod + fl + rare + loy
            print(f"    R{race_num} {driver:25} {base}+{pod}+{fl}+{rare}+{loy}={calc} \u2260 {ptotal}")
    else:
        cursor.execute("""
            SELECT COUNT(*) FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.season_id = %s AND rr.points_total > 0
        """, (season_id,))
        checked = cursor.fetchone()[0]
        if checked > 0:
            print(f"  \u2713 Plausibilität OK ({checked} Einträge geprüft)")


    # Zeitplausibilität: race_time + penalty_seconds = race_time_final
    cursor.execute("""
        SELECT d.psn_name, r.race_number,
               rr.race_time, rr.penalty_seconds, rr.race_time_final
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        JOIN drivers d ON rr.driver_id = d.driver_id
        WHERE r.season_id = %s
        AND rr.penalty_seconds > 0
        AND rr.race_time IS NOT NULL
        AND rr.race_time_final IS NOT NULL
        AND ABS(TIME_TO_SEC(rr.race_time) + rr.penalty_seconds - TIME_TO_SEC(rr.race_time_final)) > 1
        LIMIT 10
    """, (season_id,))
    time_errors = cursor.fetchall()

    print(f"\n\u23f1\ufe0f  Zeitplausibilität (race_time + strafe = race_time_final):")
    if time_errors:
        print(f"  \u26a0\ufe0f  Fehler gefunden:")
        for driver, race_num, rt, penalty, rtf in time_errors:
            print(f"    R{race_num} {driver:25} {rt} + {penalty}s \u2260 {rtf}")
    else:
        cursor.execute("""
            SELECT COUNT(*) FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.season_id = %s AND rr.penalty_seconds > 0
        """, (season_id,))
        checked = cursor.fetchone()[0]
        if checked > 0:
            print(f"  \u2713 OK ({checked} Einträge mit Strafe geprüft)")
        else:
            print(f"  \u2139\ufe0f  Keine Strafen in dieser Season")

    # Datenqualität
    issues = []

    cursor.execute("""
        SELECT r.race_number FROM races r LEFT JOIN race_results rr ON r.race_id = rr.race_id
        WHERE r.season_id = %s GROUP BY r.race_id HAVING COUNT(rr.result_id) = 0
    """, (season_id,))
    empty_races = [r[0] for r in cursor.fetchall()]
    if empty_races:
        issues.append(f"Rennen ohne Ergebnisse: {empty_races}")

    cursor.execute("""
        SELECT COUNT(*) FROM race_results rr JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.team_id IS NULL
    """, (season_id,))
    no_team = cursor.fetchone()[0]
    if no_team:
        issues.append(f"{no_team} Ergebnisse ohne Team")

    cursor.execute("""
        SELECT COUNT(*) FROM race_results rr JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.vehicle_id IS NULL
    """, (season_id,))
    no_vehicle = cursor.fetchone()[0]
    if no_vehicle:
        issues.append(f"{no_vehicle} Ergebnisse ohne Fahrzeug")

    cursor.execute("""
        SELECT COUNT(*) FROM race_results rr JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.time_percent IS NULL AND rr.status = 'FIN'
    """, (season_id,))
    no_time_pct = cursor.fetchone()[0]
    if no_time_pct:
        issues.append(f"{no_time_pct} Ergebnisse ohne time_percent (ohne DNFs)")

    cursor.execute("""
        SELECT COUNT(*) FROM race_results rr JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.finish_pos_grid IS NULL
    """, (season_id,))
    no_grid_pos = cursor.fetchone()[0]
    if no_grid_pos:
        issues.append(f"{no_grid_pos} Ergebnisse ohne finish_pos_grid")

    cursor.execute("""
        SELECT COUNT(*) FROM race_results rr
        JOIN grids g ON rr.grid_id = g.grid_id
        JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.finish_pos_grid > 16
    """, (season_id,))
    too_high = cursor.fetchone()[0]
    if too_high:
        issues.append(f"{too_high} Ergebnisse mit finish_pos_grid > 16")

    if issues:
        print(f"\n⚠️  Datenqualität:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print(f"\n✓ Datenqualität: OK")

    print("\n" + "="*70)
    cursor.close()
    conn.close()

def main():
    if len(sys.argv) < 2:
        print("Verwendung: python3 check.py <season_id> [--details]")
        sys.exit(1)

    try:
        season_id = int(sys.argv[1])
    except ValueError:
        print("✗ Season-ID muss eine Zahl sein!")
        sys.exit(1)

    required_env = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing = [var for var in required_env if not os.getenv(var)]
    if missing:
        print("✗ Fehlende Environment-Variablen:")
        for var in missing:
            print(f"  - {var}")
        sys.exit(1)

    check_season(season_id, '--details' in sys.argv)

if __name__ == '__main__':
    main()
