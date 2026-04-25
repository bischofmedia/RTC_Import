#!/usr/bin/env python3
"""
RTC All Seasons Check Script
Prüft alle Seasons auf Vollständigkeit

Verwendung:
    python3 check_all.py
"""

import os
import sys
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

def check_all_seasons():
    """Prüfe alle Seasons"""
    conn = connect_db()
    cursor = conn.cursor()
    
    print("="*90)
    print("RTC - Alle Seasons Vollständigkeits-Check")
    print("="*90)
    
    # Hole alle Seasons
    cursor.execute("""
        SELECT season_id, name, start_date, end_date
        FROM seasons
        ORDER BY season_id
    """)
    
    seasons = cursor.fetchall()
    
    if not seasons:
        print("Keine Seasons gefunden!")
        cursor.close()
        conn.close()
        return
    
    complete_seasons = []
    incomplete_seasons = []
    empty_seasons = []
    
    for season_id, season_name, start_date, end_date in seasons:
        # Hole Race-Statistik
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT r.race_id) as race_count,
                COUNT(DISTINCT rr.result_id) as result_count,
                COUNT(DISTINCT CASE WHEN rr.result_id IS NULL THEN r.race_id END) as empty_race_count
            FROM races r
            LEFT JOIN race_results rr ON r.race_id = rr.race_id
            WHERE r.season_id = %s
        """, (season_id,))
        
        race_count, result_count, empty_race_count = cursor.fetchone()
        
        if race_count == 0:
            # Keine Rennen überhaupt
            continue
        
        # Hole fehlende Rennen-Nummern
        cursor.execute("""
            SELECT race_number
            FROM races
            WHERE season_id = %s
            ORDER BY race_number
        """, (season_id,))
        
        race_numbers = sorted([r[0] for r in cursor.fetchall() if r[0]])
        
        if race_numbers:
            expected = list(range(1, max(race_numbers) + 1))
            missing_numbers = [n for n in expected if n not in race_numbers]
        else:
            missing_numbers = []
        
        # Hole Rennen ohne Ergebnisse
        cursor.execute("""
            SELECT r.race_number, r.race_date
            FROM races r
            LEFT JOIN race_results rr ON r.race_id = rr.race_id
            WHERE r.season_id = %s
            GROUP BY r.race_id
            HAVING COUNT(rr.result_id) = 0
            ORDER BY r.race_number
        """, (season_id,))
        
        empty_races = cursor.fetchall()
        
        # Datenqualitäts-Issues
        issues = []
        
        # Ergebnisse ohne finish_pos_grid
        cursor.execute("""
            SELECT COUNT(*)
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.season_id = %s AND rr.finish_pos_grid IS NULL
        """, (season_id,))
        no_grid_pos = cursor.fetchone()[0]
        
        # Ergebnisse ohne finish_pos_overall
        cursor.execute("""
            SELECT COUNT(*)
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.season_id = %s AND rr.finish_pos_overall IS NULL
        """, (season_id,))
        no_overall_pos = cursor.fetchone()[0]
        
        # Ergebnisse ohne time_percent (außer DNFs)
        cursor.execute("""
            SELECT COUNT(*)
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.season_id = %s 
            AND rr.time_percent IS NULL 
            AND (rr.status IS NULL OR rr.status != 'DNF')
        """, (season_id,))
        no_time_percent = cursor.fetchone()[0]
        
        # Ergebnisse ohne Team
        cursor.execute("""
            SELECT COUNT(*)
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.season_id = %s AND rr.team_id IS NULL
        """, (season_id,))
        no_team = cursor.fetchone()[0]
        
        # Ergebnisse ohne Fahrzeug
        cursor.execute("""
            SELECT COUNT(*)
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE r.season_id = %s AND rr.vehicle_id IS NULL
        """, (season_id,))
        no_vehicle = cursor.fetchone()[0]
        
        # Issues sammeln
        if missing_numbers:
            issues.append(f"Fehlende Rennen-Nummern: {missing_numbers}")
        if empty_races:
            issues.append(f"{len(empty_races)} Rennen ohne Ergebnisse")
        if no_grid_pos > 0:
            issues.append(f"{no_grid_pos}x finish_pos_grid fehlt")
        if no_overall_pos > 0:
            issues.append(f"{no_overall_pos}x finish_pos_overall fehlt")
        if no_time_percent > 0:
            issues.append(f"{no_time_percent}x time_percent fehlt")
        if no_team > 0:
            issues.append(f"{no_team}x team_id fehlt")
        if no_vehicle > 0:
            issues.append(f"{no_vehicle}x vehicle_id fehlt")
        
        # Kategorisierung
        season_info = {
            'id': season_id,
            'name': season_name,
            'race_count': race_count,
            'result_count': result_count,
            'empty_race_count': empty_race_count,
            'missing_numbers': missing_numbers,
            'empty_races': empty_races,
            'issues': issues,
            'start_date': start_date,
            'end_date': end_date
        }
        
        if result_count == 0:
            empty_seasons.append(season_info)
        elif issues:
            incomplete_seasons.append(season_info)
        else:
            complete_seasons.append(season_info)
    
    # Ausgabe
    print("\n" + "="*90)
    print(f"✅ VOLLSTÄNDIGE SEASONS ({len(complete_seasons)})")
    print("="*90)
    
    if complete_seasons:
        print(f"{'ID':<4} {'Name':<20} {'Zeitraum':<25} {'Rennen':<8} {'Ergebnisse'}")
        print("-"*90)
        for s in complete_seasons:
            timespan = f"{s['start_date']} - {s['end_date']}" if s['start_date'] else "-"
            print(f"{s['id']:<4} {s['name']:<20} {timespan:<25} {s['race_count']:<8} {s['result_count']}")
    else:
        print("  Keine vollständigen Seasons gefunden")
    
    print("\n" + "="*90)
    print(f"⚠️  UNVOLLSTÄNDIGE SEASONS ({len(incomplete_seasons)})")
    print("="*90)
    
    if incomplete_seasons:
        for s in incomplete_seasons:
            timespan = f"{s['start_date']} - {s['end_date']}" if s['start_date'] else "-"
            print(f"\n{s['id']}. {s['name']} ({timespan})")
            print(f"   Rennen: {s['race_count']}, Ergebnisse: {s['result_count']}")
            
            if s['issues']:
                print(f"   Probleme:")
                for issue in s['issues']:
                    print(f"     - {issue}")
            
            if s['empty_races']:
                print(f"   Leere Rennen:")
                for race_num, race_date in s['empty_races'][:5]:  # Max 5 anzeigen
                    race_num_str = str(race_num) if race_num else "?"
                    print(f"     - Rennen {race_num_str} ({race_date})")
                if len(s['empty_races']) > 5:
                    print(f"     ... und {len(s['empty_races']) - 5} weitere")
    else:
        print("  Keine unvollständigen Seasons gefunden")
    
    print("\n" + "="*90)
    print(f"❌ LEERE SEASONS ({len(empty_seasons)})")
    print("="*90)
    
    if empty_seasons:
        print(f"{'ID':<4} {'Name':<20} {'Zeitraum':<25} {'Rennen':<8}")
        print("-"*90)
        for s in empty_seasons:
            timespan = f"{s['start_date']} - {s['end_date']}" if s['start_date'] else "-"
            print(f"{s['id']:<4} {s['name']:<20} {timespan:<25} {s['race_count']:<8}")
    else:
        print("  Keine leeren Seasons gefunden")
    
    print("\n" + "="*90)
    print("ZUSAMMENFASSUNG")
    print("="*90)
    print(f"  ✅ Vollständig: {len(complete_seasons)}")
    print(f"  ⚠️  Unvollständig: {len(incomplete_seasons)}")
    print(f"  ❌ Leer: {len(empty_seasons)}")
    print(f"  Gesamt: {len(seasons)} Seasons")
    print("="*90)
    
    cursor.close()
    conn.close()

def main():
    # Prüfe Environment
    required_env = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing = [var for var in required_env if not os.getenv(var)]
    
    if missing:
        print("✗ Fehlende Environment-Variablen:")
        for var in missing:
            print(f"  - {var}")
        print("\nTipp: export $(grep -v '^#' .env | xargs)")
        sys.exit(1)
    
    check_all_seasons()

if __name__ == '__main__':
    main()
