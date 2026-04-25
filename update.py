#!/usr/bin/env python3
"""
RTC Race Update Script
Aktualisiert ein bestehendes Rennen in der DB basierend auf CSV

Verwendung:
    python3 update.py <race_id> <csv_file>
    
Beispiel:
    python3 update.py 217 data/RTC_2024.1_Kopie\ -\ 1.csv
"""

import sys
import os
import csv
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import mysql.connector
from mysql.connector import Error

# Gleiche Maps wie in rtc_import.py
VEHICLE_MAP = {
    'Alfa 4C': 1,
    'Aston Martin DBR9': 2,
    'Aston Martin Vantage': 3,
    "Audi R8 LMS '15": 4,
    "Audi R8 LMS Evo '19": 5,
    "BMW M3 '11": 6,
    "BMW M6 Endurance '16": 7,
    "BMW M6 Sprint '16": 8,
    'BMW Z4': 9,
    'Corvette C7': 10,
    'Citroen GT': 11,
    'Dodge Viper': 12,
    'Ferrari 458': 13,
    'Ford GT LM': 14,
    'Ford GT LM Test': 15,
    "Ford GT Race Car '18": 16,
    'Ford Mustang': 17,
    'Genesis X': 18,
    'Honda NSX': 19,
    'Hyundai Genesis': 20,
    'Jaguar F-TYPE': 21,
    "Lamborghini '15": 22,
    'Lexus RC F': 23,
    'Mazda Atenza': 24,
    'Mazda RX': 25,
    'McLaren 650S': 26,
    'McLaren F1 GTR': 27,
    'Mercedes AMG': 28,
    'Mercedes SLS': 29,
    'Mitsubishi Lancer': 30,
    "Nissan GT-R '13": 31,
    "Nissan GT-R '18": 32,
    "Nissan GT-R '99": 33,
    "Nissan Skyline '84": 34,
    'Peugeot RCZ': 35,
    'Peugeot VGT': 36,
    'Porsche 911 RSR': 37,
    'Renault R.S.01': 38,
    "Subaru BRZ GT300 '21": 39,
    'Subaru WRX': 40,
    'Suzuki VGT': 41,
    'Toyota FT-1 VGT': 42,
    'Toyota GR Supra': 43,
    "Toyota Supra GT500 '97": 44,
    'VW BEETLE': 45,
    'VW GTI VGT': 46,
    "Mercedes AMG GT3 '20": 47,
    "Honda NSX GT500 '00": 48,
    'Lexus RC F Prototyp': 49,
    'Ferrari 296 GT3': 50,
    "Porsche 911 GT3 R'22": 51,
    "Porsche 911 GT3 R'22": 51,
}

TEAM_NORMALIZATIONS = {
    'KotzBärTV': 'KOTZBÄR TV',
    'Rhein-Rur-Motorsport': 'RheinRur Motorsport',
}


class RTCUpdater:
    def __init__(self, race_id: int, csv_path: str):
        self.race_id = race_id
        self.csv_path = csv_path
        self.conn = None
        self.cursor = None
        self.season_id = None
        
        # Daten-Caches
        self.drivers = {}
        self.teams = {}
        self.grids = {}  # grid_class -> grid_id
        
    def connect_db(self):
        """Verbindung zur Datenbank herstellen"""
        try:
            self.conn = mysql.connector.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', '3306')),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            self.cursor = self.conn.cursor()
            print(f"✓ DB-Verbindung hergestellt")
        except Error as e:
            print(f"✗ DB-Fehler: {e}")
            sys.exit(1)
    
    def verify_race_exists(self):
        """Prüfe ob Race existiert"""
        self.cursor.execute("""
            SELECT r.season_id, s.name, r.race_number, r.race_date
            FROM races r
            JOIN seasons s ON r.season_id = s.season_id
            WHERE r.race_id = %s
        """, (self.race_id,))
        
        result = self.cursor.fetchone()
        if not result:
            print(f"✗ Race {self.race_id} nicht gefunden!")
            sys.exit(1)
        
        self.season_id, season_name, race_number, race_date = result
        print(f"✓ Race gefunden: Season {season_name}, Rennen {race_number}, {race_date}")
        return race_number, race_date
    
    def load_reference_data(self):
        """Lade Referenzdaten aus DB"""
        print("\nLade Referenzdaten...")
        
        # Drivers
        self.cursor.execute("SELECT driver_id, psn_name FROM drivers")
        for driver_id, psn_name in self.cursor.fetchall():
            self.drivers[psn_name] = driver_id
        print(f"  ✓ {len(self.drivers)} Fahrer")
        
        # Teams
        self.cursor.execute("SELECT team_id, name FROM teams")
        for team_id, name in self.cursor.fetchall():
            self.teams[name] = team_id
        print(f"  ✓ {len(self.teams)} Teams")
        
        # Grids für dieses Rennen
        self.cursor.execute("""
            SELECT grid_id, grid_class
            FROM grids
            WHERE race_id = %s
        """, (self.race_id,))
        
        for grid_id, grid_class in self.cursor.fetchall():
            self.grids[grid_class] = grid_id
        print(f"  ✓ {len(self.grids)} Grids")
    
    def parse_csv(self) -> Tuple[str, List[dict]]:
        """Parse CSV und extrahiere Metadaten + Ergebnisse"""
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Zeile 2: Rennen-Nummer
        race_number = rows[1][1].strip().rstrip('.')
        
        # Finde Header-Zeile
        header_idx = None
        for i, row in enumerate(rows):
            if len(row) > 2 and row[1] == 'Pos':
                header_idx = i
                break
        
        if header_idx is None:
            raise ValueError("Header-Zeile nicht gefunden!")
        
        # Parse Ergebnisse
        results = []
        for row in rows[header_idx + 1:]:
            if len(row) < 11:
                continue
            
            pos_str = row[1].strip()
            if not pos_str or not pos_str.isdigit():
                continue
            
            driver = row[3].strip()
            if not driver:
                continue
            
            result = {
                'finish_pos_overall': int(pos_str),
                'driver': driver,
                'team': row[4].strip(),
                'car': row[5].strip(),
                'grid_class': row[7].strip(),
                'penalty_str': row[8].strip(),
                'penalty_points': int(row[9].strip()) if row[9].strip().isdigit() else 0,
                'race_time_str': row[10].strip(),
                'points_str': row[12].strip() if len(row) > 12 else '',
            }
            results.append(result)
        
        return race_number, results
    
    def normalize_team_name(self, team_name: str) -> Optional[str]:
        """Normalisiere Team-Namen"""
        if not team_name:
            return None
        return TEAM_NORMALIZATIONS.get(team_name, team_name)
    
    def parse_time_to_seconds(self, time_str: str) -> Optional[float]:
        """Konvertiere Zeit-String zu Sekunden"""
        if not time_str or time_str == '8:00:00,000':
            return None
        
        time_str = time_str.strip().replace(',', '.')
        parts = time_str.split(':')
        
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        return None
    
    def parse_penalty(self, penalty_str: str) -> int:
        """Parse Penalty-String"""
        if not penalty_str:
            return 0
        match = re.search(r'(\d+)', penalty_str.strip())
        return int(match.group(1)) if match else 0
    
    def parse_points(self, points_str: str) -> int:
        """Parse Punkte-String"""
        if not points_str:
            return 0
        match = re.search(r'(\d+)', points_str.strip())
        return int(match.group(1)) if match else 0
    
    def delete_existing_results(self):
        """Lösche bestehende Results für dieses Rennen"""
        self.cursor.execute("""
            DELETE FROM race_results WHERE race_id = %s
        """, (self.race_id,))
        
        deleted = self.cursor.rowcount
        print(f"\n✓ {deleted} alte Ergebnisse gelöscht")
    
    def update_results(self, results: List[dict]):
        """Füge aktualisierte Results ein"""
        # Berechne finish_pos_grid
        grid_classes = {}
        for r in results:
            gc = r['grid_class']
            if gc not in grid_classes:
                grid_classes[gc] = []
            grid_classes[gc].append(r)
        
        for gc in grid_classes:
            grid_classes[gc].sort(key=lambda x: x['finish_pos_overall'])
            for i, r in enumerate(grid_classes[gc], 1):
                r['finish_pos_grid'] = i
        
        # P1 Zeit für time_percent
        p1_time = self.parse_time_to_seconds(results[0]['race_time_str'])
        
        # Hole nächste result_id
        self.cursor.execute("SELECT COALESCE(MAX(result_id), 0) + 1 FROM race_results")
        result_id = self.cursor.fetchone()[0]
        
        # Insert
        errors = []
        inserted = 0
        
        for r in results:
            driver_id = self.drivers.get(r['driver'])
            if not driver_id:
                errors.append(f"Fahrer '{r['driver']}' nicht gefunden")
                continue
            
            team_name = self.normalize_team_name(r['team'])
            team_id = self.teams.get(team_name) if team_name else None
            
            vehicle_id = VEHICLE_MAP.get(r['car'])
            if not vehicle_id:
                errors.append(f"Fahrzeug '{r['car']}' nicht in Map")
                continue
            
            grid_id = self.grids.get(r['grid_class'])
            if not grid_id:
                errors.append(f"Grid '{r['grid_class']}' nicht gefunden")
                continue
            
            race_seconds = self.parse_time_to_seconds(r['race_time_str'])
            penalty_seconds = self.parse_penalty(r['penalty_str'])
            
            if race_seconds and p1_time:
                time_percent = ((race_seconds - penalty_seconds) / p1_time) * 100
                race_time = r['race_time_str'].replace(',', '.')
            else:
                time_percent = None
                race_time = None
            
            status = 'DNF' if r['race_time_str'] == '8:00:00,000' else None
            points_total = self.parse_points(r['points_str'])
            
            self.cursor.execute("""
                INSERT INTO race_results (
                    result_id, race_id, grid_id, driver_id, vehicle_id, team_id,
                    start_pos_grid, finish_pos_grid, finish_pos_overall,
                    race_time, time_percent, rating_at_race,
                    points_base, points_bonus, points_total,
                    status, penalty_seconds, penalty_points, livery_code, car_notes
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    NULL, %s, %s,
                    %s, %s, NULL,
                    %s, 0, %s,
                    %s, %s, %s, NULL, NULL
                )
            """, (result_id, self.race_id, grid_id, driver_id, vehicle_id, team_id,
                  r['finish_pos_grid'], r['finish_pos_overall'],
                  race_time, time_percent,
                  points_total, points_total,
                  status, penalty_seconds, r['penalty_points']))
            
            result_id += 1
            inserted += 1
        
        if errors:
            print("\n⚠️  WARNUNGEN:")
            for err in errors:
                print(f"  - {err}")
        
        print(f"\n✓ {inserted} neue Ergebnisse eingefügt")
    
    def run(self):
        """Haupt-Update-Workflow"""
        print("="*60)
        print(f"RTC Race Update - Race {self.race_id}")
        print("="*60)
        
        # 1. DB-Verbindung
        self.connect_db()
        
        # 2. Race verifizieren
        db_race_number, db_race_date = self.verify_race_exists()
        
        # 3. Referenzdaten laden
        self.load_reference_data()
        
        # 4. CSV parsen
        print(f"\nParse CSV: {self.csv_path}")
        csv_race_number, results = self.parse_csv()
        
        print(f"  CSV Rennen-Nummer: {csv_race_number}")
        print(f"  DB Rennen-Nummer: {db_race_number}")
        
        if csv_race_number != str(db_race_number):
            print(f"\n⚠️  WARNUNG: Rennen-Nummern stimmen nicht überein!")
            response = input("Trotzdem fortfahren? (j/n): ")
            if response.lower() not in ['j', 'ja', 'y', 'yes']:
                print("Abgebrochen.")
                sys.exit(0)
        
        print(f"  Ergebnisse im CSV: {len(results)}")
        
        # 5. Alte Results löschen
        self.delete_existing_results()
        
        # 6. Neue Results einfügen
        print("\nFüge neue Ergebnisse ein...")
        self.update_results(results)
        
        # 7. Commit
        self.conn.commit()
        print("\n✓ Transaktion committed")
        
        # 8. Cleanup
        self.cursor.close()
        self.conn.close()
        
        print("\n" + "="*60)
        print("✓ UPDATE ERFOLGREICH ABGESCHLOSSEN")
        print("="*60)


def main():
    if len(sys.argv) != 3:
        print("Verwendung: python3 update.py <race_id> <csv_file>")
        print("Beispiel: python3 update.py 217 'data/RTC_2024.1_Kopie - 1.csv'")
        sys.exit(1)
    
    try:
        race_id = int(sys.argv[1])
    except ValueError:
        print("✗ Race-ID muss eine Zahl sein!")
        sys.exit(1)
    
    csv_path = sys.argv[2]
    
    if not os.path.exists(csv_path):
        print(f"✗ Datei nicht gefunden: {csv_path}")
        sys.exit(1)
    
    # Prüfe Environment-Variablen
    required_env = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing = [var for var in required_env if not os.getenv(var)]
    
    if missing:
        print("✗ Fehlende Environment-Variablen:")
        for var in missing:
            print(f"  - {var}")
        sys.exit(1)
    
    try:
        updater = RTCUpdater(race_id, csv_path)
        updater.run()
    except Exception as e:
        print(f"\n✗ FEHLER: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
