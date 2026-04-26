#!/usr/bin/env python3
"""
RTC Season 2020.1 Import Script - Horizontales Format (Gesamt-Ergebnisse)
Renndaten (Datum, race_id) sind direkt im Script hinterlegt.
Die bestehenden Races in der DB werden NICHT gelöscht, nur Ergebnisse neu importiert.

Verwendung:
    python3 rtc_import_2020_1.py "data/RTC_2020.1_Kopie - Gesamt-Ergebnisse.csv"

Environment-Variablen (erforderlich):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    SEASON_ID=2 (für Season 2020.1)

CSV-Struktur:
- Zeile 2: Race-Nummer in Spalte D (index 3), Track in Spalte E (index 4)
- Zeile 4/5: SR (schnellste Runde)
- Zeile 6: Header
- Zeilen 7-53: Ergebnisse
- Abstand zwischen Rennen: 10 Spalten

Spalten-Offsets (ab start_col):
- +0: Pos
- +1: Grid (1, 2, 3)
- +2: RaceTime
- +3: Driver
- +4: Team
- +5: Pkt (Punktebasis)
- +6: Clas (PRO/SP/AM - Grid-Klasse)
- +7: Gesamtpunkte
- +8: Bonuspunkte
"""

import os
import sys
import csv
import mysql.connector
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Renndaten direkt hinterlegt (aus DB ausgelesen)
RACE_DATA = {
    1:  {'race_id': 25, 'date': '2020-01-13'},
    2:  {'race_id': 26, 'date': '2020-01-20'},
    3:  {'race_id': 27, 'date': '2020-01-27'},
    4:  {'race_id': 28, 'date': '2020-02-03'},
    5:  {'race_id': 29, 'date': '2020-02-17'},
    6:  {'race_id': 30, 'date': '2020-02-24'},
    7:  {'race_id': 31, 'date': '2020-03-02'},
    8:  {'race_id': 32, 'date': '2020-03-09'},
    9:  {'race_id': 33, 'date': '2020-03-16'},
    10: {'race_id': 34, 'date': '2020-03-23'},
    11: {'race_id': 35, 'date': '2020-04-06'},
    12: {'race_id': 36, 'date': '2020-04-13'},
    13: {'race_id': 37, 'date': '2020-04-20'},
    14: {'race_id': 38, 'date': '2020-04-27'},
    15: {'race_id': 39, 'date': '2020-05-04'},
    16: {'race_id': 40, 'date': '2020-05-11'},
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

# Grid-Klassen Mapping (Clas Spalte -> grid_number)
# PRO=1, SP=2, AM=3 aber Grid-Nummer aus Spalte +1
CLASS_TO_NUMBER = {
    'PRO': '1',
    'SP': '2',
    'AM': '3',
    'AI': '1',  # AI = alte Bezeichnung für PRO
}

# Spalten-Abstand zwischen Rennen
RACE_COL_STEP = 10

# Datenzeilen: 7-53 (Index 6-52)
DATA_ROW_START = 6
DATA_ROW_END = 53


class Season2020_1Importer:
    """Import für Season 2020.1"""

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

    def get_race_column_ranges(self, rows: List[List[str]]) -> Dict[int, int]:
        """Lese Rennnummern und start_col aus Zeile 2"""
        race_cols = {}
        header_row = rows[1]  # Zeile 2 (Index 1)

        for i, cell in enumerate(header_row):
            if cell.strip().isdigit():
                race_num = int(cell.strip())
                # start_col ist 1 Spalte vor der Rennnummer (Pos-Spalte)
                start_col = i - 1
                if start_col >= 0:
                    race_cols[race_num] = start_col

        return race_cols

    def parse_time(self, time_str: str) -> Optional[str]:
        """Parse Zeitformat"""
        if not time_str or time_str.strip() in ('DNF', '-', ''):
            return None
        time_str = time_str.replace(',', '.').strip()
        # Entferne Zeilenumbrüche
        time_str = time_str.replace('\n', '').replace('\r', '')
        return time_str if time_str else None

    def parse_fastest_lap(self, rows: List[List[str]], start_col: int) -> Tuple[Optional[str], Optional[str]]:
        """Extrahiere schnellste Runde aus SR-Zeilen (Zeile 4 oder 5)"""
        # SR steht in Zeile 4 (Index 3) oder 5 (Index 4)
        for row_idx in [3, 4]:
            row = rows[row_idx]
            if len(row) > start_col and row[start_col].strip() == 'SR':
                # SR, leer, laptime, driver
                laptime = row[start_col + 2].strip() if len(row) > start_col + 2 else ''
                driver = row[start_col + 3].strip() if len(row) > start_col + 3 else ''
                if laptime and driver:
                    laptime = laptime.replace(',', '.').replace('\n', '').replace('\r', '')
                    return driver, laptime
        return None, None

    def parse_race_results(self, rows: List[List[str]], start_col: int) -> List[Dict]:
        """Parse Ergebnisse für ein Rennen"""
        results = []

        for row in rows[DATA_ROW_START:DATA_ROW_END]:
            if len(row) <= start_col:
                continue

            pos_str = row[start_col].strip()
            if not pos_str or not pos_str.isdigit():
                continue

            # Spalten-Offsets:
            # +0: Pos
            # +1: Grid (1, 2, 3)
            # +2: RaceTime
            # +3: Driver
            # +4: Team
            # +5: Pkt (Punktebasis)
            # +6: Clas (PRO/SP/AM/AI)
            # +7: Gesamtpunkte
            # +8: Bonuspunkte
            grid_num = row[start_col + 1].strip() if len(row) > start_col + 1 else ''
            race_time = row[start_col + 2].strip() if len(row) > start_col + 2 else ''
            driver = row[start_col + 3].strip() if len(row) > start_col + 3 else ''
            team = row[start_col + 4].strip() if len(row) > start_col + 4 else ''
            points_base = row[start_col + 5].strip() if len(row) > start_col + 5 else ''
            grid_class = row[start_col + 6].strip() if len(row) > start_col + 6 else ''
            points_total = row[start_col + 7].strip() if len(row) > start_col + 7 else ''
            points_bonus = row[start_col + 8].strip() if len(row) > start_col + 8 else ''

            if not driver:
                continue

            # Parse Punkte
            try:
                pts_base = int(points_base) if points_base.isdigit() else 0
            except Exception:
                pts_base = 0

            try:
                pts_total = int(points_total) if points_total.isdigit() else 0
            except Exception:
                pts_total = 0

            try:
                pts_bonus = int(points_bonus.replace('+', '')) if points_bonus.replace('+', '').isdigit() else 0
            except Exception:
                pts_bonus = 0

            result = {
                'pos': int(pos_str),
                'driver': driver,
                'car': '',  # Kein Fahrzeug in dieser CSV
                'race_time': self.parse_time(race_time),
                'team': team,
                'grid_num': grid_num,   # 1, 2, 3
                'grid_class': grid_class,  # PRO, SP, AM, AI
                'points_base': pts_base,
                'points_total': pts_total,
                'points_bonus': pts_bonus,
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

        # Lese Rennnummern und Spalten aus Zeile 2
        race_cols = self.get_race_column_ranges(rows)
        print(f"  ✓ {len(race_cols)} Rennen gefunden: {sorted(race_cols.keys())}")

        for race_num in sorted(race_cols.keys()):
            if race_num not in RACE_DATA:
                print(f"\n⚠️  Rennen {race_num}: Keine Daten hinterlegt, überspringe")
                continue

            race_data = RACE_DATA[race_num]
            self.race_id = race_data['race_id']
            start_col = race_cols[race_num]

            # Track-Name aus Zeile 2
            track_name = rows[1][start_col + 3].strip() if len(rows[1]) > start_col + 3 else ''

            print(f"\n{'='*60}")
            print(f"Rennen {race_num}: {track_name} am {race_data['date']} (race_id={self.race_id})")
            print('='*60)

            # Schnellste Runde
            fl_driver, fl_time = self.parse_fastest_lap(rows, start_col)
            if fl_driver and fl_time:
                print(f"  Schnellste Runde: {fl_time} von {fl_driver}")
                self.update_fastest_lap(fl_time, fl_driver)

            # Ergebnisse parsen
            results = self.parse_race_results(rows, start_col)
            print(f"  Ergebnisse: {len(results)}")

            if not results:
                print("  ⚠️  Keine Ergebnisse, überspringe")
                continue

            # Neue Fahrer/Teams anlegen
            self.insert_new_drivers_and_teams(results)

            # Alte Ergebnisse löschen
            self.cursor.execute("SELECT result_id FROM race_results WHERE race_id = %s", (self.race_id,))
            result_ids = [r[0] for r in self.cursor.fetchall()]
            if result_ids:
                result_ids_str = ','.join(map(str, result_ids))
                self.cursor.execute(f"DELETE FROM bonus_points WHERE result_id IN ({result_ids_str})")
            self.cursor.execute("DELETE FROM race_results WHERE race_id = %s", (self.race_id,))
            self.cursor.execute("DELETE FROM grids WHERE race_id = %s", (self.race_id,))
            self.conn.commit()

            # Grids anlegen
            grid_classes = list(set(r['grid_class'] for r in results if r['grid_class']))
            print(f"  Grid-Klassen gefunden: {sorted(grid_classes)}")
            grid_map = self.insert_grids(grid_classes)

            # Ergebnisse einfügen
            self.insert_results(results, grid_map)

            print(f"  ✓ Rennen {race_num} importiert")

    def update_fastest_lap(self, fl_time: Optional[str], fl_driver: Optional[str]):
        """Update schnellste Runde"""
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
        """Füge Grids ein"""
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (self.race_id, driver_id, team_id, None, grid_id,
                  r['pos'], r.get('finish_pos_grid'), r['race_time'], 0, status,
                  time_percent, r['points_base'], r['points_bonus'], r['points_total']))

        self.conn.commit()

    def run(self):
        """Hauptprozess"""
        print("="*60)
        print("RTC Season 2020.1 Import - Gesamt-Ergebnisse Format")
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
        print('Usage: python3 rtc_import_2020_1.py <gesamt-ergebnisse.csv>')
        sys.exit(1)

    races_csv = sys.argv[1]

    if not os.getenv('SEASON_ID'):
        print("FEHLER: SEASON_ID nicht gesetzt!")
        sys.exit(1)

    try:
        importer = Season2020_1Importer(races_csv)
        importer.run()
    except Exception as e:
        print(f"\n✗ FEHLER: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
