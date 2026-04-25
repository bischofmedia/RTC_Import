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
    """Verbindung zur Datenbank herstellen"""
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
    """Prüfe Season in DB"""
    conn = connect_db()
    cursor = conn.cursor()
    
    print("="*70)
    print(f"Season {season_id} - Status Check")
    print("="*70)
    
    # Season Info
    cursor.execute("""
        SELECT name, start_date, end_date 
        FROM seasons 
        WHERE season_id = %s
    """, (season_id,))
    
    season_info = cursor.fetchone()
    if not season_info:
        print(f"✗ Season {season_id} nicht in DB gefunden!")
        cursor.close()
        conn.close()
        return
    
    season_name, start_date, end_date = season_info
    print(f"\nSeason: {season_name}")
    print(f"Zeitraum: {start_date} bis {end_date}")
    
    # Rennen zählen
    cursor.execute("""
        SELECT COUNT(*) FROM races WHERE season_id = %s
    """, (season_id,))
    race_count = cursor.fetchone()[0]
    
    print(f"\n📊 Statistik:")
    print(f"  Rennen in DB: {race_count}")
    
    if race_count == 0:
        print(f"\n✗ Keine Rennen für Season {season_id} gefunden!")
        cursor.close()
        conn.close()
        return
    
    # Rennen-Details
    cursor.execute("""
        SELECT 
            r.race_id,
            r.race_number,
            r.race_date,
            t.name as track_name,
            t.variant,
            COUNT(rr.result_id) as result_count
        FROM races r
        LEFT JOIN tracks t ON r.track_id = t.track_id
        LEFT JOIN race_results rr ON r.race_id = rr.race_id
        WHERE r.season_id = %s
        GROUP BY r.race_id
        ORDER BY r.race_number
    """, (season_id,))
    
    races = cursor.fetchall()
    
    # Statistik
    total_results = sum(r[5] for r in races)
    avg_results = total_results / len(races) if races else 0
    
    print(f"  Gesamt-Ergebnisse: {total_results}")
    print(f"  Ø Fahrer pro Rennen: {avg_results:.1f}")
    
    # Rennen-Nummern prüfen
    race_numbers = sorted([r[1] for r in races if r[1]])
    if race_numbers:
        expected = list(range(1, max(race_numbers) + 1))
        missing = [n for n in expected if n not in race_numbers]
        
        if missing:
            print(f"\n⚠️  Fehlende Rennen-Nummern: {missing}")
        else:
            print(f"\n✓ Alle Rennen 1-{max(race_numbers)} vorhanden")
    
    # Details anzeigen
    if show_details:
        print("\n" + "="*70)
        print("Rennen-Details:")
        print("="*70)
        print(f"{'Nr':<4} {'Race ID':<8} {'Datum':<12} {'Track':<35} {'Fahrer':<7}")
        print("-"*70)
        
        for race in races:
            race_id, race_num, race_date, track_name, variant, result_count = race
            track_full = f"{track_name} {variant}" if variant else track_name
            race_num_str = str(race_num) if race_num else "?"
            
            print(f"{race_num_str:<4} {race_id:<8} {race_date} {track_full:<35} {result_count:<7}")
    
    # Grids prüfen
    cursor.execute("""
        SELECT COUNT(DISTINCT g.grid_id)
        FROM grids g
        JOIN races r ON g.race_id = r.race_id
        WHERE r.season_id = %s
    """, (season_id,))
    grid_count = cursor.fetchone()[0]
    
    # Grid-Verteilung
    cursor.execute("""
        SELECT g.grid_class, COUNT(DISTINCT g.grid_id) as count
        FROM grids g
        JOIN races r ON g.race_id = r.race_id
        WHERE r.season_id = %s
        GROUP BY g.grid_class
        ORDER BY g.grid_class
    """, (season_id,))
    
    grid_dist = cursor.fetchall()
    
    print(f"\n📋 Grids:")
    print(f"  Gesamt: {grid_count}")
    if grid_dist:
        print(f"  Verteilung:")
        for grid_class, count in grid_dist:
            print(f"    Grid {grid_class}: {count}x")
    
    # Fahrer & Teams
    cursor.execute("""
        SELECT COUNT(DISTINCT driver_id)
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s
    """, (season_id,))
    driver_count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(DISTINCT team_id)
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND team_id IS NOT NULL
    """, (season_id,))
    team_count = cursor.fetchone()[0]
    
    print(f"\n👥 Teilnehmer:")
    print(f"  Fahrer: {driver_count}")
    print(f"  Teams: {team_count}")
    
    # DNFs
    cursor.execute("""
        SELECT COUNT(*)
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.status = 'DNF'
    """, (season_id,))
    dnf_count = cursor.fetchone()[0]
    
    if dnf_count > 0:
        print(f"\n🔧 DNFs: {dnf_count}")
    
    # Datenqualität prüfen
    issues = []
    
    # Rennen ohne Ergebnisse
    cursor.execute("""
        SELECT r.race_id, r.race_number
        FROM races r
        LEFT JOIN race_results rr ON r.race_id = rr.race_id
        WHERE r.season_id = %s
        GROUP BY r.race_id
        HAVING COUNT(rr.result_id) = 0
    """, (season_id,))
    
    empty_races = cursor.fetchall()
    if empty_races:
        issues.append(f"Rennen ohne Ergebnisse: {[r[1] for r in empty_races]}")
    
    # Ergebnisse ohne Team
    cursor.execute("""
        SELECT COUNT(*)
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.team_id IS NULL
    """, (season_id,))
    
    no_team_count = cursor.fetchone()[0]
    if no_team_count > 0:
        issues.append(f"{no_team_count} Ergebnisse ohne Team")
    
    # Ergebnisse ohne Fahrzeug
    cursor.execute("""
        SELECT COUNT(*)
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE r.season_id = %s AND rr.vehicle_id IS NULL
    """, (season_id,))
    
    no_vehicle_count = cursor.fetchone()[0]
    if no_vehicle_count > 0:
        issues.append(f"{no_vehicle_count} Ergebnisse ohne Fahrzeug")
    
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
        print("Beispiel: python3 check.py 12")
        print("Beispiel: python3 check.py 12 --details")
        sys.exit(1)
    
    try:
        season_id = int(sys.argv[1])
    except ValueError:
        print("✗ Season-ID muss eine Zahl sein!")
        sys.exit(1)
    
    show_details = '--details' in sys.argv
    
    # Prüfe Environment
    required_env = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing = [var for var in required_env if not os.getenv(var)]
    
    if missing:
        print("✗ Fehlende Environment-Variablen:")
        for var in missing:
            print(f"  - {var}")
        print("\nTipp: export $(grep -v '^#' .env | xargs)")
        sys.exit(1)
    
    check_season(season_id, show_details)

if __name__ == '__main__':
    main()
