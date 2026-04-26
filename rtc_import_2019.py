#!/usr/bin/env python3
"""
RTC Season 2019 Import Script - Vertikales Fahrer-Format
Renndaten (Datum, race_id) sind direkt im Script hinterlegt.
Die bestehenden Races in der DB werden NICHT gelöscht, nur Ergebnisse neu importiert.

Verwendung:
    python3 rtc_import_2019.py "data/RTC_2019_Kopie - Ergebnisse.csv"

Environment-Variablen (erforderlich):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    SEASON_ID=1 (für Season 2019)

CSV-Struktur:
- Zeile 1 (Index 0): Track-Namen ab col 2, Abstand 4
- Zeile 3 (Index 2): Fastest LAP - "Fastest LAP", Fahrer-ID, Zeit ab col 3, Abstand 4
- Zeilen 5-99 (Index 4-98): Fahrer-ID(col 0), Name(col 1), pro Rennen: Lobby(+0) ab col 2, Abstand 4
- Zeile 101 (Index 100): Header für Ergebnisse
- Zeilen 102-196 (Index 101-195): Name(col 1), pro Rennen: Zeit(+0), Rang(+1), Strafe?(+2) ab col 2, Abstand 4
- 99:99,999 oder x = nicht mitgefahren/DNF

Grids: L1 und L2 (aus Lobby-Spalte der Zeilen 5-99)
"""

import os
import sys
import csv
import mysql.connector
from typing import Dict, List, Optional, Tuple

# Renndaten direkt hinterlegt (aus DB ausgelesen)
RACE_DATA = {
    1:  {'race_id': 1,  'date': '2019-07-01'},
    2:  {'race_id': 2,  'date': '2019-07-08'},
    3:  {'race_id': 3,  'date': '2019-07-15'},
    4:  {'race_id': 4,  'date': '2019-07-22'},
    5:  {'race_id': 5,  'date': '2019-07-29'},
    6:  {'race_id': 6,  'date': '2019-08-05'},
    7:  {'race_id': 7,  'date': '2019-08-12'},
    8:  {'race_id': 8,  'date': '2019-08-19'},
    9:  {'race_id': 9,  'date': '2019-08-26'},
    10: {'race_id': 10, 'date': '2019-09-02'},
    11: {'race_id': 11, 'date': '2019-09-09'},
    12: {'race_id': 12, 'date': '2019-09-16'},
    13: {'race_id': 13, 'date': '2019-09-23'},
    14: {'race_id': 14, 'date': '2019-09-30'},
    15: {'race_id': 15, 'date': '2019-10-07'},
    16: {'race_id': 16, 'date': '2019-10-14'},
    17: {'race_id': 17, 'date': '2019-10-21'},
    18: {'race_id': 18, 'date': '2019-10-28'},
    19: {'race_id': 19, 'date': '2019-11-04'},
    20: {'race_id': 20, 'date': '2019-11-11'},
    21: {'race_id': 21, 'date': '2019-11-18'},
    22: {'race_id': 22, 'date': '2019-11-25'},
    23: {'race_id': 23, 'date': '2019-12-02'},
    24: {'race_id': 24, 'date': '2019-12-09'},
}

# Team-Normalisierung
TEAM_NORMALIZATIONS = {
    'KotzBärTV': 'KOTZBÄR TV',
}

# Fahrer-Normalisierung
DRIVER_NORMALIZATIONS = {
    'PrimeApeX21': 'PrimeapeX21',
}

# Anzahl Rennen
NUM_RACES = 24

# Zeilen-Bereiche (0-basiert)
LOBBY_ROW_START = 4    # Zeile 5
LOBBY_ROW_END = 99     # Zeile 99 (inkl.)
RESULT_ROW_START = 101 # Zeile 102
RESULT_ROW_END = 196   # Zeile 196 (inkl.)

# Spalten-Abstand pro Rennen
RACE_COL_STEP = 4

# Start-Spalte für Renndaten
RACE_COL_BASE = 2  # Rennen 1 startet bei col 2


class Season2019Importer:

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
        print("\nLade Referenzdaten...")

        self.cursor.execute("SELECT driver_id, psn_name FROM drivers")
        self.drivers = {psn: did for did, psn in self.cursor.fetchall()}
        print(f"  ✓ {len(self.drivers)} Fahrer")

        self.cursor.execute("SELECT team_id, name FROM teams")
        self.teams = {name: tid for tid, name in self.cursor.fetchall()}
        print(f"  ✓ {len(self.teams)} Teams")

    def parse_time(self, time_str: str) -> Optional[str]:
        """Parse Zeitformat - gibt None zurück für DNF/nicht gefahren"""
        if not time_str:
            return None
        time_str = time_str.replace(',', '.').replace('\n', '').replace('\r', '').strip()
        if time_str in ('x', '', '99:99.999', '99:99,999'):
            return None
        # Füge führende Stunde hinzu wenn nötig (z.B. 50:49.862 -> 0:50:49.862)
        if time_str.count(':') == 1:
            time_str = '0:' + time_str
        return time_str

    def get_col(self, race_num: int) -> int:
        """Berechne Start-Spalte für ein Rennen"""
        return RACE_COL_BASE + (race_num - 1) * RACE_COL_STEP

    def build_driver_id_map(self, rows: List[List[str]]) -> Dict[str, str]:
        """Baue Map: Fahrer-ID -> Fahrername aus Zeilen 5-99"""
        id_map = {}
        for row in rows[LOBBY_ROW_START:LOBBY_ROW_END + 1]:
            if len(row) < 2:
                continue
            driver_id = row[0].strip()
            driver_name = row[1].strip()
            if driver_id and driver_name and driver_id.isdigit():
                id_map[driver_id] = driver_name
        return id_map

    def get_lobby_for_driver(self, rows: List[List[str]], driver_name: str, race_num: int) -> Optional[str]:
        """Hole Lobby (L1/L2) für Fahrer und Rennen aus Zeilen 5-99"""
        col = self.get_col(race_num)
        for row in rows[LOBBY_ROW_START:LOBBY_ROW_END + 1]:
            if len(row) < 2:
                continue
            name = row[1].strip()
            if name == driver_name:
                lobby = row[col].strip() if len(row) > col else ''
                if lobby in ('1', '2'):
                    return f'L{lobby}'
                return None
        return None

    def parse_fastest_lap(self, rows: List[List[str]], race_num: int, id_map: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
        """Extrahiere schnellste Runde aus Zeile 3"""
        row = rows[2]  # Zeile 3 (Index 2)
        col = self.get_col(race_num)

        # Format: "Fastest LAP", Fahrer-ID, Zeit
        # col-1 = "Fastest LAP", col = Fahrer-ID, col+1 = Zeit
        try:
            label = row[col - 1].strip() if len(row) > col - 1 else ''
            if label != 'Fastest LAP':
                return None, None
            driver_id = row[col].strip() if len(row) > col else ''
            laptime = row[col + 1].strip() if len(row) > col + 1 else ''

            if not driver_id or not laptime:
                return None, None

            driver_name = id_map.get(driver_id)
            laptime = laptime.replace(',', '.').replace('\n', '').replace('\r', '')

            return driver_name, laptime
        except Exception:
            return None, None

    def parse_race_results(self, rows: List[List[str]], race_num: int) -> List[Dict]:
        """Parse Ergebnisse aus Zeilen 102-196"""
        col = self.get_col(race_num)
        results_raw = []

        for row in rows[RESULT_ROW_START:RESULT_ROW_END + 1]:
            if len(row) < 2:
                continue

            driver_name = row[1].strip()
            if not driver_name:
                continue

            # Normalisiere Fahrername
            if driver_name in DRIVER_NORMALIZATIONS:
                driver_name = DRIVER_NORMALIZATIONS[driver_name]

            race_time_str = row[col].strip() if len(row) > col else ''
            pos_str = row[col + 1].strip() if len(row) > col + 1 else ''

            race_time = self.parse_time(race_time_str)

            # Position
            try:
                pos = int(pos_str)
            except Exception:
                continue

            # Überspringe wenn nicht mitgefahren (pos > Teilnehmerzahl oder time = 99:99)
            if race_time is None and race_time_str not in ('x', ''):
                # Hat eine Zeit die nicht gültig ist = DNF
                pass

            results_raw.append({
                'driver': driver_name,
                'race_time': race_time,
                'pos': pos,
                'is_dnf': race_time is None and race_time_str not in ('', 'x'),
                'not_participated': race_time is None and race_time_str in ('x', ''),
            })

        # Filtere nicht teilgenommene Fahrer
        results = [r for r in results_raw if not r['not_participated']]

        # Sortiere nach Position
        results.sort(key=lambda x: x['pos'])

        # Vergib finale Positionen (1, 2, 3...)
        # Fahrer mit gültiger Zeit kommen zuerst, dann DNFs
        finishers = [r for r in results if r['race_time'] is not None]
        dnfs = [r for r in results if r['race_time'] is None]

        final_results = []
        for i, r in enumerate(finishers):
            r['final_pos'] = i + 1
            final_results.append(r)
        for i, r in enumerate(dnfs):
            r['final_pos'] = len(finishers) + i + 1
            final_results.append(r)

        return final_results

    def process_all_races(self):
        print(f"\nParse Races CSV: {self.races_csv}")

        with open(self.races_csv, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        # Baue Fahrer-ID Map
        id_map = self.build_driver_id_map(rows)
        print(f"  ✓ {len(id_map)} Fahrer-IDs gefunden")

        # Track-Namen aus Zeile 1
        track_row = rows[0]

        for race_num in range(1, NUM_RACES + 1):
            if race_num not in RACE_DATA:
                continue

            race_data = RACE_DATA[race_num]
            self.race_id = race_data['race_id']
            col = self.get_col(race_num)

            # Track-Name
            track_name = track_row[col].strip() if len(track_row) > col else ''

            print(f"\n{'='*60}")
            print(f"Rennen {race_num}: {track_name} am {race_data['date']} (race_id={self.race_id})")
            print('='*60)

            # Schnellste Runde
            fl_driver, fl_time = self.parse_fastest_lap(rows, race_num, id_map)
            if fl_driver and fl_time:
                print(f"  Schnellste Runde: {fl_time} von {fl_driver}")
                self.update_fastest_lap(fl_time, fl_driver)

            # Ergebnisse
            results = self.parse_race_results(rows, race_num)
            print(f"  Ergebnisse: {len(results)}")

            if not results:
                print("  ⚠️  Keine Ergebnisse, überspringe")
                continue

            # Lobby für jeden Fahrer ermitteln
            for r in results:
                lobby = self.get_lobby_for_driver(rows, r['driver'], race_num)
                r['grid_class'] = lobby if lobby else 'L1'  # Fallback L1

            self.insert_new_drivers(results)

            # Lösche alte Ergebnisse
            self.cursor.execute("SELECT result_id FROM race_results WHERE race_id = %s", (self.race_id,))
            result_ids = [r[0] for r in self.cursor.fetchall()]
            if result_ids:
                result_ids_str = ','.join(map(str, result_ids))
                self.cursor.execute(f"DELETE FROM bonus_points WHERE result_id IN ({result_ids_str})")
            self.cursor.execute("DELETE FROM race_results WHERE race_id = %s", (self.race_id,))
            self.cursor.execute("DELETE FROM grids WHERE race_id = %s", (self.race_id,))
            self.conn.commit()

            # Grids anlegen
            grid_classes = list(set(r['grid_class'] for r in results))
            print(f"  Grid-Klassen: {sorted(grid_classes)}")
            grid_map = self.insert_grids(grid_classes)

            self.insert_results(results, grid_map)

            print(f"  ✓ Rennen {race_num} importiert")

    def update_fastest_lap(self, fl_time: Optional[str], fl_driver: Optional[str]):
        if fl_driver in DRIVER_NORMALIZATIONS:
            fl_driver = DRIVER_NORMALIZATIONS[fl_driver]
        fl_driver_id = self.drivers.get(fl_driver)
        fl_time = fl_time.replace(',', '.') if fl_time else None
        if fl_time and fl_time.count(':') == 1:
            fl_time = '0:' + fl_time
        self.cursor.execute("""
            UPDATE races SET fastest_lap_time = %s, fastest_lap_driver_id = %s
            WHERE race_id = %s
        """, (fl_time, fl_driver_id, self.race_id))
        self.conn.commit()

    def insert_new_drivers(self, results: List[Dict]):
        new_drivers = []
        for r in results:
            if r['driver'] and r['driver'] not in self.drivers:
                new_drivers.append(r['driver'])

        for driver_name in new_drivers:
            self.cursor.execute("INSERT IGNORE INTO drivers (psn_name) VALUES (%s)", (driver_name,))

        if new_drivers:
            self.conn.commit()
            self.cursor.execute("SELECT driver_id, psn_name FROM drivers")
            self.drivers = {psn: did for did, psn in self.cursor.fetchall()}
            print(f"  ✓ {len(new_drivers)} neue Fahrer")

    def insert_grids(self, grid_classes: List[str]) -> Dict[str, int]:
        """Füge Grids ein: L1 -> grid_number=1, L2 -> grid_number=2"""
        grid_map = {}
        grid_number_map = {'L1': '1', 'L2': '2'}

        for gc in sorted(grid_classes):
            grid_number = grid_number_map.get(gc, '1')

            self.cursor.execute("""
                INSERT INTO grids (race_id, grid_number, grid_class)
                VALUES (%s, %s, %s)
            """, (self.race_id, grid_number, gc))

            grid_map[gc] = self.cursor.lastrowid

        return grid_map

    def calculate_time_percent(self, results: List[Dict]):
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
        # Berechne finish_pos_grid
        grid_positions = {}
        for r in sorted(results, key=lambda x: x['final_pos']):
            gc = r['grid_class']
            if gc not in grid_positions:
                grid_positions[gc] = 0
            grid_positions[gc] += 1
            r['finish_pos_grid'] = grid_positions[gc]

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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0)
            """, (self.race_id, driver_id, None, None, grid_id,
                  r['final_pos'], r.get('finish_pos_grid'), r['race_time'], 0, status,
                  time_percent))

        self.conn.commit()

    def run(self):
        print("="*60)
        print("RTC Season 2019 Import")
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
        print('Usage: python3 rtc_import_2019.py <ergebnisse.csv>')
        sys.exit(1)

    races_csv = sys.argv[1]

    if not os.getenv('SEASON_ID'):
        print("FEHLER: SEASON_ID nicht gesetzt!")
        sys.exit(1)

    try:
        importer = Season2019Importer(races_csv)
        importer.run()
    except Exception as e:
        print(f"\n✗ FEHLER: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
