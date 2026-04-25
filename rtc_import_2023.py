#!/usr/bin/env python3
"""
RTC Rennen Import Script - 2023 Format Version
Für Seasons bis einschließlich 2023 (anderes CSV-Format)

Unterschiede zu 2024+ Format:
- RaceTime in Spalte 9 (statt 10)
- Punkte in Spalte 11 (statt 12)
- Keine Penalty Points Spalte

Verwendung:
    python3 rtc_import_2023.py data/rennen5.csv

Environment-Variablen (erforderlich):
    DB_HOST         - MySQL Host (z.B. localhost)
    DB_PORT         - MySQL Port (z.B. 3306)
    DB_NAME         - Datenbank Name (z.B. d046d457)
    DB_USER         - Datenbank User
    DB_PASSWORD     - Datenbank Passwort
    SEASON_ID       - Saison ID (z.B. 11 für 2023.3)
"""

import sys
import os
import csv
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import mysql.connector
from mysql.connector import Error

# Fahrzeug-Mapping (Name im CSV -> vehicle_id)
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
}

# Track-Name-Mapping (CSV-Name -> DB-Suchstring)
TRACK_NAME_MAP = {
    'Blue Moon Bay - A': ('Blue Moon Bay Speedway', 'Infield A'),
    'Blue Moon Bay - B': ('Blue Moon Bay Speedway', 'Infield B'),
    'Brands Hatch - GP': ('Brands Hatch', 'GP Circuit'),
    'Brands Hatch - Indy': ('Brands Hatch', 'Indy Circuit'),
    'Barcelona': ('Barcelona', 'GP'),
    'Daytona - Road': ('Daytona', 'Road Course'),
    'Dragon Trail - Gardens': ('Dragon Trail', 'Gardens'),
    'Dragon Trail - Seaside': ('Dragon Trail', 'Seaside'),
    'Fuji': ('Fuji International Speedway', 'Full Course'),
    'Kyoto - Miyabi': ('Kyoto DP', 'Miyabi'),
    'Kyoto - Yamagiwa': ('Kyoto DP', 'Yamagiwa'),
    'Lago Maggiore - GP': ('Lago Maggiore', 'GP'),
    'Laguna Seca': ('Laguna Seca', 'Full Course'),
    'Le Mans': ('LeMans', 'Full Course'),
    'Monza': ('Monza', 'Full Course'),
    'Mount Panorama': ('Mount Panorama', 'Full Course'),
    'Nürburgring - GP': ('Nürburgring', 'GP'),
    'Nürburgring - Nordschleife': ('Nürburgring', 'Nordschleife'),
    'Red Bull Ring': ('Red Bull Ring', 'Full Course'),
    'Road Atlanta': ('Road Atlanta', 'Full Course'),
    'Sardegna - A': ('Sardegna - Road Track', 'A'),
    'Spa-Francorchamps': ('Spa-Francorchamps', 'Full Course'),
    'Suzuka': ('Suzuka Circuit', 'Full Course'),
    'Tokyo - East': ('Tokyo Expressway', 'East Clockwise'),
    'Trial Mountain': ('Trial Mountain', 'Full Course'),
    'Watkins Glen': ('Watkins Glen', 'Long Course'),
    'Grand Valley South': ('Grand Valley', 'South'),
    'Kyoto DP - Yamagiwa + Miyabi': ('Kyoto DP', 'Yamagiwa + Miyabi'),
    'Tokyo Expressway South-CW': ('Tokyo Expressway', 'South Counter-Clockwise'),
    'Brands Hatch GP': ('Brands Hatch', 'GP Circuit'),
    'Nürburgring GP/F': ('Nürburgring', 'GP'),
    'Deep Forest Raceway -REVERSE': ('Deep Forest Raceway', 'Reverse'),
    'Alsace - Village': ('Alsace', 'Village'),
    'Barcelona - GP - no chicane': ('Barcelona', 'GP (no chicane)'),
    'Tokyo Expressway East_CCW': ('Tokyo Expressway', 'East Counter-Clockwise'),
    'Daytona Straßenkurs': ('Daytona', 'Road Course'),
    'Sainte-Croix - B': ('Sainte-Croix', 'Layout B'),
    'Sardegna - B': ('Sardegna - Road Track', 'B'),
    'Lago Maggiore - GP REV': ('Lago Maggiore', 'GP Reverse'),
    'Autopolis IRC': ('Autopolis', 'IRC Full Course'),
    'Kyoto DP - Yamagiwa': ('Kyoto DP', 'Yamagiwa'),
    'Fuji Int. Speedway (Short)': ('Fuji International Speedway', 'Short Course'),
    'Watkins Glen Short': ('Watkins Glen', 'Short Course'),
}

# Team-Namen-Normalisierung
TEAM_NORMALIZATIONS = {
    'KotzBärTV': 'KOTZBÄR TV',
    'Rhein-Rur-Motorsport': 'RheinRur Motorsport',
}


class RTCImporter:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.conn = None
        self.cursor = None
        self.season_id = None
        self.race_id = None
        self.grid_id_start = None
        self.result_id_start = None
        
        # Daten-Caches
        self.drivers = {}
        self.teams = {}
        self.tracks = {}
        self.versions = []
        
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
            self.season_id = int(os.getenv('SEASON_ID'))
            print(f"✓ DB-Verbindung hergestellt (Season {self.season_id})")
        except Error as e:
            print(f"✗ DB-Fehler: {e}")
            sys.exit(1)
    
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
        
        # Tracks
        self.cursor.execute("SELECT track_id, name, variant FROM tracks")
        for track_id, name, variant in self.cursor.fetchall():
            key = (name, variant) if variant else (name,)
            self.tracks[key] = track_id
        print(f"  ✓ {len(self.tracks)} Tracks")
        
        # Game Versions (für Datum-Mapping)
        self.cursor.execute("""
            SELECT version_id, release_date 
            FROM game_versions 
            WHERE game = 'Gran Turismo 7'
            ORDER BY release_date DESC
        """)
        self.versions = [(vid, rdate) for vid, rdate in self.cursor.fetchall()]
        print(f"  ✓ {len(self.versions)} GT7 Versionen")
    
    def get_next_ids(self):
        """Hole nächste verfügbare IDs"""
        # Race ID
        self.cursor.execute("SELECT COALESCE(MAX(race_id), 0) + 1 FROM races")
        self.race_id = self.cursor.fetchone()[0]
        
        # Grid ID
        self.cursor.execute("SELECT COALESCE(MAX(grid_id), 0) + 1 FROM grids")
        self.grid_id_start = self.cursor.fetchone()[0]
        
        # Result ID
        self.cursor.execute("SELECT COALESCE(MAX(result_id), 0) + 1 FROM race_results")
        self.result_id_start = self.cursor.fetchone()[0]
        
        print(f"\nNächste IDs:")
        print(f"  Race ID: {self.race_id}")
        print(f"  Grid ID: {self.grid_id_start}")
        print(f"  Result ID: {self.result_id_start}")
    
    def parse_csv(self) -> Tuple[str, datetime, str, str, List[dict]]:
        """Parse CSV und extrahiere Metadaten + Ergebnisse"""
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Zeile 2: Rennen-Nummer
        race_number = rows[1][1].strip().rstrip('.')
        
        # Zeile 3: Track, Datum, Fastest Lap
        track_name = rows[1][4].strip()
        race_date_str = rows[2][4].strip()
        fastest_lap_time = rows[2][6].strip()
        fastest_lap_driver = rows[2][8].strip()
        
        # Parse Datum (Format: DD.MM.YYYY)
        race_date = datetime.strptime(race_date_str, '%d.%m.%Y')
        
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
                'penalty_points': 0,  # Penalty Points in 2023 nicht in CSV
                'race_time_str': row[9].strip(),  # SPALTE 9 statt 10!
                'points_str': row[11].strip() if len(row) > 11 else '',  # SPALTE 11 statt 12!
            }
            results.append(result)
        
        return race_number, race_date, track_name, fastest_lap_time, fastest_lap_driver, results
    
    def map_track(self, track_name: str) -> int:
        """Mappe Track-Name aus CSV zu track_id"""
        # Exaktes Mapping versuchen
        if track_name in TRACK_NAME_MAP:
            track_tuple = TRACK_NAME_MAP[track_name]
            if track_tuple in self.tracks:
                return self.tracks[track_tuple]
        
        # Fallback: Suche nach Name-Match
        for key, track_id in self.tracks.items():
            if track_name.lower() in ' '.join(key).lower():
                return track_id
        
        raise ValueError(f"Track '{track_name}' nicht gefunden!")
    
    def get_version_for_date(self, race_date: datetime) -> int:
        """Finde GT7-Version die zum Renndatum aktiv war"""
        # Konvertiere datetime zu date für Vergleich
        race_date_only = race_date.date() if hasattr(race_date, 'date') else race_date
        for version_id, release_date in self.versions:
            if race_date_only >= release_date:
                return version_id
        return self.versions[-1][0]  # Fallback: älteste Version
    
    def normalize_team_name(self, team_name: str) -> Optional[str]:
        """Normalisiere Team-Namen"""
        if not team_name:
            return None
        return TEAM_NORMALIZATIONS.get(team_name, team_name)
    
    def insert_new_teams(self, team_names: set) -> int:
        """Füge neue Teams ein"""
        new_teams = []
        for team in team_names:
            if team and team not in self.teams:
                new_teams.append(team)
        
        if not new_teams:
            return 0
        
        max_team_id = max(self.teams.values()) if self.teams else 0
        
        for i, team in enumerate(sorted(new_teams), 1):
            new_id = max_team_id + i
            self.cursor.execute("""
                INSERT INTO teams (team_id, name, founded_season_id, is_active)
                VALUES (%s, %s, %s, 1)
            """, (new_id, team, self.season_id))
            self.teams[team] = new_id
            print(f"    + Neues Team: {team} (ID {new_id})")
        
        return len(new_teams)
    
    def insert_new_drivers(self, driver_names: set) -> int:
        """Füge neue Fahrer ein"""
        new_drivers = []
        for driver in driver_names:
            if driver and driver not in self.drivers:
                new_drivers.append(driver)
        
        if not new_drivers:
            return 0
        
        max_driver_id = max(self.drivers.values()) if self.drivers else 0
        
        for i, driver in enumerate(sorted(new_drivers), 1):
            new_id = max_driver_id + i
            self.cursor.execute("""
                INSERT IGNORE INTO drivers (driver_id, psn_name, first_season_id, is_active)
                VALUES (%s, %s, %s, 1)
            """, (new_id, driver, self.season_id))
            self.drivers[driver] = new_id
            print(f"    + Neuer Fahrer: {driver} (ID {new_id})")
        
        return len(new_drivers)
    
    def insert_race(self, race_number: str, race_date: datetime, track_id: int, version_id: int, 
                    fastest_lap_time: str, fastest_lap_driver: str):
        """Füge Race ein"""
        fl_driver_id = self.drivers.get(fastest_lap_driver)
        
        self.cursor.execute("""
            INSERT INTO races (race_id, season_id, race_number, track_id, version_id, race_date, 
                             fastest_lap_time, fastest_lap_driver_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (self.race_id, self.season_id, race_number, track_id, version_id, race_date.date(),
              fastest_lap_time, fl_driver_id))
    
    def insert_grids(self, grid_classes: list) -> dict:
        """Füge Grids ein, return grid_class -> grid_id mapping"""
        grid_map = {}
        
        for i, gc in enumerate(sorted(grid_classes)):
            grid_id = self.grid_id_start + i
            # grid_number = grid_class (damit 2a und 2b unterschiedlich sind)
            grid_num = gc
            
            self.cursor.execute("""
                INSERT INTO grids (grid_id, race_id, grid_number, grid_class, grid_label)
                VALUES (%s, %s, %s, %s, %s)
            """, (grid_id, self.race_id, grid_num, gc, f'Grid {gc}'))
            
            grid_map[gc] = grid_id
        
        return grid_map
    
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
    
    def insert_results(self, results: List[dict], grid_map: dict):
        """Füge Race Results ein"""
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
        
        # Insert
        result_id = self.result_id_start
        errors = []
        
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
            
            grid_id = grid_map[r['grid_class']]
            
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
        
        if errors:
            print("\n⚠️  WARNUNGEN:")
            for err in errors:
                print(f"  - {err}")
    
    def validate(self):
        """Validiere Import"""
        self.cursor.execute("""
            SELECT COUNT(*) FROM race_results WHERE race_id = %s
        """, (self.race_id,))
        count = self.cursor.fetchone()[0]
        
        print(f"\n✓ Validierung: {count} Ergebnisse importiert")
        
        self.cursor.execute("""
            SELECT g.grid_class, COUNT(*) as count
            FROM grids g
            JOIN race_results r ON g.grid_id = r.grid_id
            WHERE g.race_id = %s
            GROUP BY g.grid_class
            ORDER BY g.grid_class
        """, (self.race_id,))
        
        print("  Grid-Verteilung:")
        for grid_class, count in self.cursor.fetchall():
            print(f"    Grid {grid_class}: {count} Fahrer")
    
    def run(self):
        """Haupt-Import-Workflow"""
        print("="*60)
        print("RTC Rennen Import - Production")
        print("="*60)
        
        # 1. DB-Verbindung
        self.connect_db()
        
        # 2. Referenzdaten laden
        self.load_reference_data()
        
        # 3. Nächste IDs holen
        self.get_next_ids()
        
        # 4. CSV parsen
        print(f"\nParse CSV: {self.csv_path}")
        race_num, race_date, track_name, fl_time, fl_driver, results = self.parse_csv()
        
        print(f"  Rennen: {race_num}")
        print(f"  Datum: {race_date.strftime('%d.%m.%Y')}")
        print(f"  Track: {track_name}")
        print(f"  Schnellste Runde: {fl_time} von {fl_driver}")
        print(f"  Ergebnisse: {len(results)}")
        
        # 5. Track & Version mappen
        track_id = self.map_track(track_name)
        version_id = self.get_version_for_date(race_date)
        print(f"  Track ID: {track_id}")
        print(f"  Version ID: {version_id}")
        
        # 6. Neue Teams/Fahrer prüfen
        csv_teams = set(self.normalize_team_name(r['team']) for r in results if r['team'])
        csv_drivers = set(r['driver'] for r in results if r['driver'])
        
        print("\nPrüfe neue Daten...")
        new_team_count = self.insert_new_teams(csv_teams)
        new_driver_count = self.insert_new_drivers(csv_drivers)
        
        # WICHTIG: Commit und Cache neu laden
        if new_team_count > 0 or new_driver_count > 0:
            self.conn.commit()
            # Cache neu laden damit die neuen IDs verfügbar sind
            self.cursor.execute("SELECT driver_id, psn_name FROM drivers")
            self.drivers = {psn: did for did, psn in self.cursor.fetchall()}
            self.cursor.execute("SELECT team_id, name FROM teams")
            self.teams = {name: tid for tid, name in self.cursor.fetchall()}
            print("  ✓ Neue Teams/Fahrer committed und Cache aktualisiert")
        
        if new_team_count == 0 and new_driver_count == 0:
            print("  ✓ Keine neuen Teams oder Fahrer")
        
        # 7. Race einfügen
        print("\nFüge Race ein...")
        self.insert_race(race_num, race_date, track_id, version_id, fl_time, fl_driver)
        print(f"  ✓ Race {self.race_id} angelegt")
        
        # 8. Grids einfügen
        grid_classes = list(set(r['grid_class'] for r in results))
        print(f"\nFüge {len(grid_classes)} Grids ein...")
        grid_map = self.insert_grids(grid_classes)
        for gc, gid in sorted(grid_map.items()):
            print(f"  ✓ Grid {gc}: ID {gid}")
        
        # 9. Results einfügen
        print(f"\nFüge {len(results)} Results ein...")
        self.insert_results(results, grid_map)
        print("  ✓ Results eingefügt")
        
        # 10. Commit
        self.conn.commit()
        print("\n✓ Transaktion committed")
        
        # 11. Validierung
        self.validate()
        
        # 12. Cleanup
        self.cursor.close()
        self.conn.close()
        
        print("\n" + "="*60)
        print("✓ IMPORT ERFOLGREICH ABGESCHLOSSEN")
        print("="*60)


def main():
    if len(sys.argv) != 2:
        print("Verwendung: python3 rtc_import.py <csv_file>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    if not os.path.exists(csv_path):
        print(f"✗ Datei nicht gefunden: {csv_path}")
        sys.exit(1)
    
    # Prüfe Environment-Variablen
    required_env = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'SEASON_ID']
    missing = [var for var in required_env if not os.getenv(var)]
    
    if missing:
        print("✗ Fehlende Environment-Variablen:")
        for var in missing:
            print(f"  - {var}")
        sys.exit(1)
    
    try:
        importer = RTCImporter(csv_path)
        importer.run()
    except Exception as e:
        print(f"\n✗ FEHLER: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
