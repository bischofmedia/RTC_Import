#!/usr/bin/env python3
"""
RTC Season 2020.2 Import Script - Horizontal Format
Renndaten (Datum, Track) sind direkt im Script hinterlegt (kein Streams.csv)
Die bestehenden Races in der DB werden NICHT gelöscht, nur die Ergebnisse neu importiert.

Verwendung:
    python3 rtc_import_2020_2.py "data/RTC_2020.2_Kopie - Races.csv"

Environment-Variablen (erforderlich):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    SEASON_ID=3 (für Season 2020.2)

Unterschiede zu 2020.3:
- Kein Streams.csv - Renndaten direkt im Script
- RACE_COL_STEP = 18
- Bestehende race_ids aus DB werden verwendet
- Grid-Klasse in offset +9 (1, 2a, 2b, 2c, 3)
"""

import os
import sys
import csv
import mysql.connector
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Track-Mapping
TRACK_NAME_MAP = {
    'Mount Panorama': ('Mount Panorama', 'Full Course'),
    'Kyoto DP Yamagiwa Reverse': ('Kyoto DP', 'Yamagiwa Reverse'),
    'Suzuka Circuit': ('Suzuka Circuit', 'Full Course'),
    'Tokyo Expressway South Clockwise': ('Tokyo Expressway', 'South Clockwise'),
    'Laguna Seca': ('Laguna Seca', 'Full Course'),
    'Blue Moon Bay Infield A': ('Blue Moon Bay Speedway', 'Infield A'),
    'Interlagos': ('Autódromo De Interlagos', 'Full Course'),
    'Dragon Trail Seaside': ('Dragon Trail', 'Seaside'),
    'Spa-Francorchamps': ('Spa-Francorchamps', 'Full Course'),
    'Sardegna A': ('Sardegna - Road Track', 'A'),
    'Sainte-Croix B': ('Sainte-Croix', 'Layout B'),
    'Alsace Village': ('Alsace', 'Village'),
    'Brands Hatch GP': ('Brands Hatch', 'GP Circuit'),
    'Lago Maggiore Center': ('Lago Maggiore', 'Center'),
    'Red Bull Ring': ('Red Bull Ring', 'Full Course'),
    'Nürburgring Nordschleife': ('Nürburgring', 'Nordschleife'),
}

# Renndaten direkt hinterlegt (aus DB ausgelesen)
# race_num -> {race_id, date, track_id}
RACE_DATA = {
    1:  {'race_id': 89,  'date': '2020-05-25', 'track_id': 48},   # Mount Panorama
    2:  {'race_id': 90,  'date': '2020-06-01', 'track_id': 30},   # Kyoto DP Yamagiwa Reverse
    3:  {'race_id': 91,  'date': '2020-06-08', 'track_id': 72},   # Suzuka Circuit
    4:  {'race_id': 92,  'date': '2020-06-15', 'track_id': 78},   # Tokyo Expressway South Clockwise
    5:  {'race_id': 93,  'date': '2020-06-22', 'track_id': 43},   # Laguna Seca
    6:  {'race_id': 94,  'date': '2020-06-29', 'track_id': 6},    # Blue Moon Bay Infield A
    7:  {'race_id': 95,  'date': '2020-07-06', 'track_id': 3},    # Interlagos
    8:  {'race_id': 96,  'date': '2020-07-13', 'track_id': 24},   # Dragon Trail Seaside
    9:  {'race_id': 97,  'date': '2020-07-20', 'track_id': 70},   # Spa-Francorchamps
    10: {'race_id': 98,  'date': '2020-07-27', 'track_id': 64},   # Sardegna A
    11: {'race_id': 99,  'date': '2020-08-03', 'track_id': 60},   # Sainte-Croix B
    12: {'race_id': 100, 'date': '2020-08-10', 'track_id': 2},    # Alsace Village
    13: {'race_id': 101, 'date': '2020-08-17', 'track_id': 12},   # Brands Hatch GP
    14: {'race_id': 102, 'date': '2020-08-24', 'track_id': 40},   # Lago Maggiore Center
    15: {'race_id': 103, 'date': '2020-08-31', 'track_id': 55},   # Red Bull Ring
    16: {'race_id': 104, 'date': '2020-09-28', 'track_id': 53},   # Nürburgring Nordschleife
}

# Vehicle-Mapping
VEHICLE_MAP = {
    '': 10,
    'Aston Martin Vantage': 3,
    'Aston Martin DBR9': 2,
    'Mercedes-Benz AMG': 28,
    'Chevrolet CORVETTE C7': 10,
    'Porsche 911 RSR': 37,
    'Mazda ATENZA': 24,
    'Honda NSX': 19,
    'BMW M6': 7,
    'BMW Z4': 9,
    'Nissan GT-R NISMO': 32,
    'Jaguar F-TYPE': 21,
    'Volkswagen BEETLE': 45,
    'Volkswagen GTI VGT': 46,
    'Mitsubishi LANCER EVO': 30,
    'Hyundai GENESIS': 20,
    'Toyota FT-1 VGT': 42,
    'Mazda RX': 25,
    'Mercedes-Benz SLS AMG': 29,
    'Peugeot RCZ': 35,
    'Lexus RC F': 23,
    'Ford MUSTANG': 17,
    'Toyota GR Supra': 43,
    'Subaru WRX': 40,
    'Audi R8 LMS': 4,
    'Ford GT LM SPEC II': 14,
    'Alfa Romeo 4C': 1,
    'McLaren F1 GTR': 27,
    'Peugeot VGT': 36,
    'McLaren 650S': 26,
    'Ferrari 458 ITALIA': 13,
    'Lamborghini HURACAN': 22,
    'Dodge VIPER SRT': 12,
    "Nissan GT-R N24 '13": 31,
    'BMW M3 GT': 6,
    'Citroen GT': 11,
    'Renault Sport R.S.01': 38,
}

# Team-Normalisierung
TEAM_NORMALIZATIONS = {
    'KotzBärTV': 'KOTZBÄR TV',
    'Rhein-Rur-Motorsport': 'RheinRur Motorsport',
    'Rhein-Ruhr-Motosport': 'RheinRur Motorsport',
}

# Spalten-Abstand zwischen Rennen
RACE_COL_STEP = 18

# Max Zeilen pro Rennen
MAX_DATA_ROW = 63


class Season2020_2Importer:
    """Import für Season 2020.2 mit horizontalem Format"""

    def __init__(self, races_csv: str):
        self.races_csv = races_csv
        self.season_id = int(os.getenv('SEASON_ID'))

        self.conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', '3306')),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        self.cursor = self.conn.cursor()

        self.drivers = {}
        self.teams = {}
        self.race_id = None

    def load_reference_data(self):
        """Lade Fahrer und Teams"""
        print("\nLade Referenzdaten...")

        self.cursor.execute("SELECT driver_id, psn_name FROM drivers")
        self.drivers = {psn: did for did, psn in self.cursor.fetchall()}
        print(f"  ✓ {len(self.drivers)} Fahrer")

        self.cursor.execute("SELECT team_id, name FROM teams")
        self.teams = {name: tid for tid, name in self.cursor.fetchall()}
        print(f"  ✓ {len(self.teams)} Teams")

    def parse_time(self, time_str: str) -> Optional[str]:
        """Parse Zeitformat"""
        if not time_str or time_str.strip() in ('DNF', '-', ''):
            return None
        time_str = time_str.replace(',', '.')
        return time_str.strip()

    def parse_fastest_lap(self, sr_row: List[str], start_col: int) -> Tuple[Optional[str], Optional[str]]:
        """Extrahiere schnellste Runde aus SR-Zeile"""
        try:
            driver_col = start_col + 1
            time_col = start_col + 5

            if len(sr_row) <= time_col:
                return None, None

            driver = sr_row[driver_col].strip()
            laptime = sr_row[time_col].strip()

            if not driver or not laptime:
                return None, None

            laptime = laptime.replace(',', '.')
            return driver, laptime
        except Exception:
            return None, None

    def parse_race_results(self, data_rows: List[List[str]], start_col: int) -> List[Dict]:
        """Parse Ergebnisse für ein Rennen"""
        results = []

        for row in data_rows[5:MAX_DATA_ROW]:
            if len(row) <= start_col:
                continue

            pos_str = row[start_col].strip()
            if not pos_str or not pos_str.isdigit():
                continue

            # Layout:
            # +0: Pos, +1: Driver, +2: ?, +3: Car
            # +4: Livery, +5: RaceTime, +6: Diff, +7: Team
            # +8: CL (PRO/SP/AM - ignorieren)
            # +9: Grid (1, 2a, 2b, 2c, 3)
            driver = row[start_col + 1].strip() if len(row) > start_col + 1 else ''
            car = row[start_col + 3].strip() if len(row) > start_col + 3 else ''
            race_time = row[start_col + 5].strip() if len(row) > start_col + 5 else ''
            team = row[start_col + 7].strip() if len(row) > start_col + 7 else ''
            grid_class = row[start_col + 9].strip() if len(row) > start_col + 9 else ''

            if not driver:
                continue

            result = {
                'pos': int(pos_str),
                'driver': driver,
                'car': car,
                'race_time': self.parse_time(race_time),
                'team': team,
                'grid_class': grid_class,
                'time_percent': None,
                'finish_pos_grid': None,
            }

            results.append(result)

        return results

    def process_all_races(self):
        """Verarbeite alle Rennen"""

        print(f"\nParse Races CSV: {self.races_csv}")

        with open(self.races_csv, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        sr_row = rows[3]

        for race_num in range(1, 17):
            if race_num not in RACE_DATA:
                print(f"\n⚠️  Rennen {race_num}: Keine Daten hinterlegt, überspringe")
                continue

            race_data = RACE_DATA[race_num]
            self.race_id = race_data['race_id']
            start_col = 1 + ((race_num - 1) * RACE_COL_STEP)

            print(f"\n{'='*60}")
            print(f"Rennen {race_num}: race_id={self.race_id}, start_col={start_col}")
            print('='*60)

            fl_driver, fl_time = self.parse_fastest_lap(sr_row, start_col)
            if fl_driver and fl_time:
                print(f"  Schnellste Runde: {fl_time} von {fl_driver}")
                self.update_fastest_lap(fl_time, fl_driver)

            results = self.parse_race_results(rows, start_col)
            print(f"  Ergebnisse: {len(results)}")

            if not results:
                print("  ⚠️  Keine Ergebnisse, überspringe")
                continue

            self.insert_new_drivers_and_teams(results)

            # Lösche alte Ergebnisse für dieses Rennen
            self.cursor.execute("SELECT result_id FROM race_results WHERE race_id = %s", (self.race_id,))
            result_ids = [r[0] for r in self.cursor.fetchall()]
            if result_ids:
                result_ids_str = ','.join(map(str, result_ids))
                self.cursor.execute(f"DELETE FROM bonus_points WHERE result_id IN ({result_ids_str})")
            self.cursor.execute("DELETE FROM race_results WHERE race_id = %s", (self.race_id,))
            self.cursor.execute("DELETE FROM grids WHERE race_id = %s", (self.race_id,))
            self.conn.commit()

            grid_classes = list(set(r['grid_class'] for r in results if r['grid_class']))
            print(f"  Grid-Klassen gefunden: {sorted(grid_classes)}")
            grid_map = self.insert_grids(grid_classes)

            self.insert_results(results, grid_map)

            print(f"  ✓ Rennen {race_num} importiert")

    def update_fastest_lap(self, fl_time: Optional[str], fl_driver: Optional[str]):
        """Update schnellste Runde in Race"""
        fl_driver_id = None
        if fl_driver and fl_driver in self.drivers:
            fl_driver_id = self.drivers[fl_driver]

        self.cursor.execute("""
            UPDATE races SET fastest_lap_time = %s, fastest_lap_driver_id = %s
            WHERE race_id = %s
        """, (fl_time, fl_driver_id, self.race_id))
        self.conn.commit()

    def insert_new_drivers_and_teams(self, results: List[Dict]):
        """Füge neue Fahrer/Teams ein"""
        new_drivers = []
        new_teams = []

        for r in results:
            team = r['team']
            if team in TEAM_NORMALIZATIONS:
                team = TEAM_NORMALIZATIONS[team]
                r['team'] = team

            if r['driver'] and r['driver'] not in self.drivers:
                new_drivers.append(r['driver'])

            if team and team not in self.teams:
                new_teams.append(team)

        for team_name in new_teams:
            self.cursor.execute("INSERT IGNORE INTO teams (name) VALUES (%s)", (team_name,))

        for driver_name in new_drivers:
            self.cursor.execute(
                "INSERT IGNORE INTO drivers (psn_name) VALUES (%s)",
                (driver_name,)
            )

        if new_teams or new_drivers:
            self.conn.commit()

            self.cursor.execute("SELECT driver_id, psn_name FROM drivers")
            self.drivers = {psn: did for did, psn in self.cursor.fetchall()}
            self.cursor.execute("SELECT team_id, name FROM teams")
            self.teams = {name: tid for tid, name in self.cursor.fetchall()}

            print(f"  ✓ {len(new_teams)} neue Teams, {len(new_drivers)} neue Fahrer")

    def insert_grids(self, grid_classes: List[str]) -> Dict[str, int]:
        """Füge Grids ein - grid_class direkt übernehmen"""
        grid_map = {}

        for gc in sorted(grid_classes):
            self.cursor.execute("""
                INSERT INTO grids (race_id, grid_number, grid_class)
                VALUES (%s, %s, %s)
            """, (self.race_id, gc, gc))

            grid_map[gc] = self.cursor.lastrowid

        return grid_map

    def calculate_time_percent(self, results: List[Dict]):
        """Berechne time_percent für alle Fahrer"""

        def time_to_seconds(time_str: str) -> float:
            if not time_str:
                return 0
            parts = time_str.split(':')
            if len(parts) == 3:
                h, m, s = parts
                return int(h) * 3600 + int(m) * 60 + float(s)
            return 0

        time_seconds = []
        for r in results:
            if r['race_time']:
                seconds = time_to_seconds(r['race_time'])
                if seconds > 0:
                    time_seconds.append((r, seconds))

        if not time_seconds:
            return

        fastest_seconds = min(ts[1] for ts in time_seconds)

        for r, seconds in time_seconds:
            r['time_percent'] = (seconds / fastest_seconds) * 100

    def insert_results(self, results: List[Dict], grid_map: Dict[str, int]):
        """Füge Results ein"""

        # Berechne finish_pos_grid
        grid_positions = {}
        for r in sorted(results, key=lambda x: x['pos']):
            gc = r['grid_class']
            if gc not in grid_positions:
                grid_positions[gc] = 0
            grid_positions[gc] += 1
            r['finish_pos_grid'] = grid_positions[gc]

        # Berechne time_percent
        self.calculate_time_percent(results)

        seen_drivers = set()

        for r in results:
            driver_id = self.drivers.get(r['driver'])

            if not driver_id:
                print(f"  ⚠️  Fahrer '{r['driver']}' nicht gefunden")
                continue

            if driver_id in seen_drivers:
                print(f"  ⚠️  Fahrer '{r['driver']}' doppelt, überspringe")
                continue
            seen_drivers.add(driver_id)

            team_id = self.teams.get(r['team']) if r['team'] else None
            grid_id = grid_map.get(r['grid_class'])
            vehicle_id = VEHICLE_MAP.get(r['car'])

            if vehicle_id is None:
                print(f"  ⚠️  Fahrzeug '{r['car']}' nicht in Map")
                continue

            if grid_id is None:
                print(f"  ⚠️  Grid '{r['grid_class']}' nicht gefunden, überspringe")
                continue

            status = 'DNF' if r['race_time'] is None else 'FIN'
            time_percent = r.get('time_percent')

            self.cursor.execute("""
                INSERT INTO race_results
                (race_id, driver_id, team_id, vehicle_id, grid_id,
                 finish_pos_overall, finish_pos_grid, race_time, penalty_seconds, status,
                 time_percent, points_base, points_bonus, points_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0)
            """, (self.race_id, driver_id, team_id, vehicle_id, grid_id,
                  r['pos'], r.get('finish_pos_grid'), r['race_time'], 0, status, time_percent))

        self.conn.commit()

    def run(self):
        """Hauptprozess"""
        print("="*60)
        print("RTC Season 2020.2 Import - Horizontal Format")
        print("="*60)
        print(f"✓ DB-Verbindung hergestellt (Season {self.season_id})")

        self.load_reference_data()
        self.process_all_races()

        print("\n" + "="*60)
        print("✓ IMPORT ABGESCHLOSSEN")
        print("="*60)

        self.cursor.close()
        self.conn.close()


def main():
    if len(sys.argv) != 2:
        print('Usage: python3 rtc_import_2020_2.py <races.csv>')
        sys.exit(1)

    races_csv = sys.argv[1]

    if not os.getenv('SEASON_ID'):
        print("FEHLER: SEASON_ID nicht gesetzt!")
        sys.exit(1)

    try:
        importer = Season2020_2Importer(races_csv)
        importer.run()
    except Exception as e:
        print(f"\n✗ FEHLER: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
