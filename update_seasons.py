#!/usr/bin/env python3
"""
RTC Seasons Update Script
Fügt start_date und end_date zu seasons-Tabelle hinzu

Verwendung:
    python3 update_seasons.py
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

def update_seasons():
    """Füge start_date und end_date zu seasons hinzu"""
    conn = connect_db()
    cursor = conn.cursor()
    
    print("="*70)
    print("Seasons-Tabelle updaten")
    print("="*70)
    
    # Prüfe ob Spalten bereits existieren
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = 'seasons' 
        AND COLUMN_NAME IN ('start_date', 'end_date')
    """, (os.getenv('DB_NAME'),))
    
    existing_columns = [row[0] for row in cursor.fetchall()]
    
    # Spalten hinzufügen falls nicht vorhanden
    if 'start_date' not in existing_columns:
        print("\n1. Füge Spalte 'start_date' hinzu...")
        cursor.execute("""
            ALTER TABLE seasons 
            ADD COLUMN start_date DATE NULL AFTER name
        """)
        print("   ✓ Spalte 'start_date' hinzugefügt")
    else:
        print("\n✓ Spalte 'start_date' existiert bereits")
    
    if 'end_date' not in existing_columns:
        print("\n2. Füge Spalte 'end_date' hinzu...")
        cursor.execute("""
            ALTER TABLE seasons 
            ADD COLUMN end_date DATE NULL AFTER start_date
        """)
        print("   ✓ Spalte 'end_date' hinzugefügt")
    else:
        print("\n✓ Spalte 'end_date' existiert bereits")
    
    conn.commit()
    
    # Hole alle Seasons
    cursor.execute("SELECT season_id, name FROM seasons ORDER BY season_id")
    seasons = cursor.fetchall()
    
    print(f"\n3. Aktualisiere Daten für {len(seasons)} Seasons...\n")
    
    updated = 0
    skipped = 0
    
    for season_id, season_name in seasons:
        # Hole erstes und letztes Renndatum
        cursor.execute("""
            SELECT 
                MIN(race_date) as start_date,
                MAX(race_date) as end_date,
                COUNT(*) as race_count
            FROM races 
            WHERE season_id = %s
        """, (season_id,))
        
        result = cursor.fetchone()
        
        if result and result[0]:  # Wenn Rennen vorhanden
            start_date, end_date, race_count = result
            
            # Update Season
            cursor.execute("""
                UPDATE seasons 
                SET start_date = %s, end_date = %s 
                WHERE season_id = %s
            """, (start_date, end_date, season_id))
            
            print(f"   Season {season_id:2d} ({season_name:20s}): {start_date} bis {end_date} ({race_count} Rennen)")
            updated += 1
        else:
            print(f"   Season {season_id:2d} ({season_name:20s}): Keine Rennen gefunden - übersprungen")
            skipped += 1
    
    conn.commit()
    
    print("\n" + "="*70)
    print(f"✓ Fertig!")
    print(f"  {updated} Seasons aktualisiert")
    if skipped > 0:
        print(f"  {skipped} Seasons übersprungen (keine Rennen)")
    print("="*70)
    
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
    
    # Bestätigung einholen
    print("\nDieses Script wird:")
    print("  1. Die Spalten 'start_date' und 'end_date' zur seasons-Tabelle hinzufügen (falls nicht vorhanden)")
    print("  2. Für jede Season das erste und letzte Renndatum aus der races-Tabelle eintragen")
    print()
    
    response = input("Fortfahren? (j/n): ")
    
    if response.lower() not in ['j', 'ja', 'y', 'yes']:
        print("Abgebrochen.")
        sys.exit(0)
    
    update_seasons()

if __name__ == '__main__':
    main()
