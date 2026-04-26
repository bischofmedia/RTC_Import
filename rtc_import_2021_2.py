#!/usr/bin/env python3
"""
RTC Season 2021.2 Import Script - Horizontal Format
Für die spezielle horizontale Tabellenstruktur wo alle 16 Rennen nebeneinander liegen

Benötigt 2 CSV-Dateien:
- Races.csv: Horizontale Ergebnisse (alle Rennen nebeneinander)
- Streams.csv: Renndaten (Datum, Track)

Verwendung:
    python3 rtc_import_2021_2.py "data/RTC_2021.2_Kopie - Races.csv" "data/RTC_2021.2_Kopie - Streams.csv"

Environment-Variablen (erforderlich):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    SEASON_ID=6 (für Season 2021.2)
"""

import os
import sys
import csv
import mysql.connector
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Track-Mapping
TRACK_NAME_MAP = {
    'Mount Panorama': ('Mount Panorama', 'Full Course'),
    'Tokyo Expressway South-Out': ('Tokyo Expressway', 'South Counter-Clockwise'),
    'Fuji Int. Speedway (Short)': ('Fuji International Speedway', 'Short'),
    'Autopolis IRC': ('Autopolis IRC', 'Full Course'),
    'Autódromo De Interlagos': ('Autódromo De Interlagos', 'Full Course'),
    'Blue Moon Bay - B REV': ('Blue Moon Bay Speedway', 'Infield B Reverse'),
    'Red Bull Ring': ('Red Bull Ring', 'Full Course'),
    'Red Bull Ring Wet': ('Red Bull Ring', 'Full Course'),
    'Nürburgring GP/F': ('Nürburgring', 'GP'),
    'Sainte-Croix - B': ('Sainte-Croix', 'Layout B'),
    'Spa-Francorchamps': ('Spa-Francorchamps', 'Full Course'),
    'Sardegna - A - REV': ('Sardegna - Road Track', 'A Reverse'),
    'Monza': ('Monza', 'Full Course'),
    'Lago Maggiore - GP': ('Lago Maggiore', 'GP'),
    'Dragon Trail - Seaside': ('Dragon Trail', 'Seaside'),
    'Brands Hatch GP': ('Brands Hatch', 'GP Circuit'),
    'Nürburgring 24h': ('Nürburgring', '24h Layout'),
    'Deep Forest Raceway': ('Deep Forest Raceway', 'Full Course'),
    'Watkins Glen': ('Watkins Glen', 'Full Course'),
    'Barcelona': ('Circuit de Barcelona-Catalunya', 'Full Course'),
    'Daytona': ('Daytona International Speedway', 'Road Course'),
}

# Vehicle-Mapping
VEHICLE_MAP = {
    '': 10,  # Fallback -> Corvette C7
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
}

# Team-Normalisierung
TEAM_NORMALIZATIONS = {
    'KotzBärTV': 'KOTZBÄR TV',
    'Rhein-Rur-Motorsport': 'RheinRur Motorsport',
    'Rhein-Ruhr-Motosport': 'RheinRur Motorsport',
}

# Grid-Mapping
CLASS_TO_NUMBER = {
    'PRO': '1',
    'SP': '2',
    'AM': '3',
}

# Spalten-Abstand zwischen Rennen (Season 2021.2)
RACE_COL_STEP = 17


class Season2021_2Importer:
    """Import für Season 2021.2 mit horizontalem Format"""

    def __init__(self, races_csv: str, streams_csv: str):
        self.races_csv = races_csv
        self.streams_csv = streams_csv
        self.season_id = int(os.getenv('SEASON_ID'))

        # DB-Verbindung
        self.conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', '3306')),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        self.cursor = self.conn.cursor()

        # Caches
        self.drivers = {}
        self.teams = {}
        self.tracks = {}
        self.versions = []
        self.race_id = None

    def load_reference_data(self):
        """Lade Fahrer, Teams, Tracks, Versionen"""
        print("\nLade Referenzdaten...")

        self.cursor.execute("SELECT driver_id, psn_name FROM drivers")
        self.drivers = {psn: did for did, psn in self.cursor.fetchall()}
        print(f"  ✓ {len(self.drivers)} Fahrer")

        self.cursor.execute("SELECT team_id, name FROM teams")
        self.teams = {name: tid for tid, name in self.cursor.fetchall()}
        print(f"  ✓ {len(self.teams)} Teams")

        self.cursor.execute("SELECT track_id, name, variant FROM tracks")
        self.tracks = {(name, variant if variant else ''): tid
                       for tid, name, variant in self.cursor.fetchall()}
        print(f"  ✓ {len(self.tracks)} Tracks")

        self.cursor.execute("SELECT version_id, release_date FROM game_versions ORDER BY release_date DESC")
        self.versions = list(self.cursor.fetchall())
        print(f"  ✓ {len(self.versions)} Game Versionen")

    def parse_streams_csv(self) -> Dict[int, Dict]:
        """Parse Streams.csv für Datum + Track pro Rennen"""
        print(f"\nParse Streams CSV: {self.streams_csv}")

        races_info = {}

        with open(self.streams_csv, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

            for row in rows[3:]:
                if len(row) < 6:
                    continue

                race_num_str = row[2].strip()
                if not race_num_str or not race_num_str.isdigit():
                    continue

                race_num = int(race_num_str)
                date_str = row[4].strip()
                track_str = row[5].strip()

                if not date_str or not track_str:
                    continue

                try:
                    race_date = datetime.strptime(date_str, '%d.%m.%Y')
                except Exception:
                    print(f"  ⚠️  Warnung: Ungültiges Datum für Rennen {race_num}: {date_str}")
                    continue

                races_info[race_num] = {
                    'date': race_date,
                    'track_name': track_str
                }

        print(f"  ✓ {len(races_info)} Rennen gefunden")
        return races_info

    def parse_time(self, time_str: str) -> Optional[str]:
        """Parse Zeitformat"""
        if not time_str or time_str.strip() in ('DNF', '-', ''):
            return None
        time_str = time_str.replace(',', '.')
        return time_str.strip()

    def parse_fastest_lap(self, sr_row: List[str], start_col: int) -> Tuple[Optional[str], Optional[str]]:
        """Extrahiere schnellste Runde aus SR-Zeile"""
        try:
            # Season 2021.2: SR, DRIVER, , CAR, , LAPTIME
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

        # Season 2021.2: Daten ab Zeile 6 (Index 5)
        for row in data_rows[5:]:
            if len(row) <= start_col:
                continue

            pos_str = row[start_col].strip()
            if not pos_str or not pos_str.isdigit():
                continue

            # Season 2021.2 Layout (kein NAT-Feld):
            # +0: Pos
            # +1: Driver
            # +2: (leer)
            # +3: Car
            # +4: Livery
            # +5: RaceTime
            # +6: Diff
            # +7: Team
            # +8: CL (Grid-Klasse)
            nat = ''
            driver = row[start_col + 1].strip() if len(row) > start_col + 1 else ''
            car = row[start_col + 3].strip() if len(row) > start_col + 3 else ''
            race_time = row[start_col + 5].strip() if len(row) > start_col + 5 else ''
            team = row[start_col + 7].strip() if len(row) > start_col + 7 else ''
            grid_class = row[start_col + 8].strip() if len(row) > start_col + 8 else ''

            if not driver:
                continue

            result = {
                'pos': int(pos_str),
                'nat': nat,
                'driver': driver,
                'car': car,
                'race_time': self.parse_time(race_time),
                'penalty': '',
                'team': team,
                'grid_class': grid_class,
                'time_percent': None,
            }

            results.append(result)

        return results

    def process_all_races(self):
        """Verarbeite alle Rennen"""

        races_info = self.parse_streams_csv()

        print(f"\nParse Races CSV: {self.races_csv}")

        with open(self.races_csv, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        sr_row = rows[3]  # Zeile 4 = SR (schnellste Runde)

        for i in range(16):
            race_num = i + 1
            start_col = 1 + (i * RACE_COL_STEP)

            if race_num not in races_info:
                print(f"\n⚠️  Rennen {race_num}: Keine Daten in Streams.csv, überspringe")
                continue

            info = races_info[race_num]

            print(f"\n{'='*60}")
            print(f"Rennen {race_num}: {info['track_name']} am {info['date'].strftime('%d.%m.%Y')}")
            print('='*60)

            fl_driver, fl_time = self.parse_fastest_lap(sr_row, start_col)
            if fl_driver and fl_time:
                print(f"  Schnellste Runde: {fl_time} von {fl_driver}")

            results = self.parse_race_results(rows, start_col)
            print(f"  Ergebnisse: {len(results)}")

            if not results:
                print("  ⚠️  Keine Ergebnisse, überspringe")
                continue

            track_id = self.map_track(info['track_name'])
            print(f"  Track ID: {track_id}")

            version_id = self.get_version_for_date(info['date'])
            print(f"  Version ID: {version_id}")

            self.insert_new_drivers_and_teams(results)

            self.insert_race(race_num, info['date'], track_id, version_id, fl_time, fl_driver)

            grid_classes = list(set(r['grid_class'] for r in results if r['grid_class'] in CLASS_TO_NUMBER))
            print(f"  Grid-Klassen gefunden: {grid_classes}")
            grid_map = self.insert_grids(grid_classes)

            self.insert_results(results, grid_map)

            print(f"  ✓ Rennen {race_num} importiert")

    def map_track(self, track_name: str) -> int:
        """Mappe Track-Name zu track_id"""
        if track_name in TRACK_NAME_MAP:
            track_tuple = TRACK_NAME_MAP[track_name]
            if track_tuple in self.tracks:
                return self.tracks[track_tuple]

        raise ValueError(f"Track '{track_name}' nicht gefunden!")

    def get_version_for_date(self, race_date: datetime) -> int:
        """Finde Game-Version zum Datum"""
        race_date_only = race_date.date()
        for version_id, release_date in self.versions:
            if race_date_only >= release_date:
                return version_id
        return self.versions[-1][0]

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
                new_drivers.append((r['driver'], r['nat']))

            if team and team not in self.teams:
                new_teams.append(team)

        for team_name in new_teams:
            self.cursor.execute("INSERT IGNORE INTO teams (name) VALUES (%s)", (team_name,))

        for driver_name, nat in new_drivers:
            self.cursor.execute(
                "INSERT IGNORE INTO drivers (psn_name, nat) VALUES (%s, %s)",
                (driver_name, nat if nat else None)
            )

        if new_teams or new_drivers:
            self.conn.commit()

            self.cursor.execute("SELECT driver_id, psn_name FROM drivers")
            self.drivers = {psn: did for did, psn in self.cursor.fetchall()}
            self.cursor.execute("SELECT team_id, name FROM teams")
            self.teams = {name: tid for tid, name in self.cursor.fetchall()}

            print(f"  ✓ {len(new_teams)} neue Teams, {len(new_drivers)} neue Fahrer")

    def insert_race(self, race_num: int, race_date: datetime, track_id: int,
                    version_id: int, fl_time: Optional[str], fl_driver: Optional[str]):
        """Füge Race ein"""
        fl_driver_id = None
        if fl_driver and fl_driver in self.drivers:
            fl_driver_id = self.drivers[fl_driver]

        self.cursor.execute("""
            INSERT INTO races (season_id, race_number, race_date, track_id, version_id,
                             fastest_lap_time, fastest_lap_driver_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (self.season_id, race_num, race_date, track_id, version_id, fl_time, fl_driver_id))

        self.race_id = self.cursor.lastrowid
        self.conn.commit()

    def insert_grids(self, grid_classes: List[str]) -> Dict[str, int]:
        """Füge Grids ein, returns {grid_class: grid_id}"""
        grid_map = {}

        for gc in sorted(grid_classes):
            grid_number = CLASS_TO_NUMBER.get(gc, '1')

            self.cursor.execute("""
                INSERT INTO grids (race_id, grid_number, grid_class)
                VALUES (%s, %s, %s)
            """, (self.race_id, grid_number, gc))

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

        self.calculate_time_percent(results)

        for r in results:
            driver_id = self.drivers.get(r['driver'])
            team_id = self.teams.get(r['team']) if r['team'] else None
            grid_id = grid_map.get(r['grid_class'])
            vehicle_id = VEHICLE_MAP.get(r['car'])

            if not driver_id:
                print(f"  ⚠️  Fahrer '{r['driver']}' nicht gefunden")
                continue

            if vehicle_id is None:
                print(f"  ⚠️  Fahrzeug '{r['car']}' nicht in Map")
                continue

            if grid_id is None:
                print(f"  ⚠️  Grid-Klasse '{r['grid_class']}' nicht gefunden, überspringe")
                continue

            status = 'DNF' if r['race_time'] is None else 'FIN'
            time_percent = r.get('time_percent')

            self.cursor.execute("""
                INSERT INTO race_results
                (race_id, driver_id, team_id, vehicle_id, grid_id,
                 finish_pos_overall, race_time, penalty_seconds, status,
                 time_percent, points_base, points_bonus, points_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0)
            """, (self.race_id, driver_id, team_id, vehicle_id, grid_id,
                  r['pos'], r['race_time'], 0, status, time_percent))

        self.conn.commit()

    def run(self):
        """Hauptprozess"""
        print("="*60)
        print("RTC Season 2021.2 Import - Horizontal Format")
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
    if len(sys.argv) != 3:
        print('Usage: python3 rtc_import_2021_2.py <races.csv> <streams.csv>')
        sys.exit(1)

    races_csv = sys.argv[1]
    streams_csv = sys.argv[2]

    if not os.getenv('SEASON_ID'):
        print("FEHLER: SEASON_ID nicht gesetzt!")
        sys.exit(1)

    try:
        importer = Season2021_2Importer(races_csv, streams_csv)
        importer.run()
    except Exception as e:
        print(f"\n✗ FEHLER: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
