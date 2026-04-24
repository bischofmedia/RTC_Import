# RTC Import - Projektübersicht

## System-Architektur

```
┌─────────────────┐
│   CSV-Datei     │
│  (Rennergebnis) │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│     rtc_import.py                   │
│  ┌───────────────────────────────┐  │
│  │ 1. CSV parsen                 │  │
│  │ 2. Referenzdaten laden (DB)   │  │
│  │ 3. Track mappen               │  │
│  │ 4. Version ermitteln (Datum)  │  │
│  │ 5. Neue Teams/Fahrer anlegen  │  │
│  │ 6. Race + Grids einfügen      │  │
│  │ 7. Results einfügen           │  │
│  │ 8. Validieren                 │  │
│  └───────────────────────────────┘  │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│   MySQL/MariaDB Datenbank           │
│  ┌───────────────────────────────┐  │
│  │ races                         │  │
│  │ grids                         │  │
│  │ race_results                  │  │
│  │ drivers                       │  │
│  │ teams                         │  │
│  │ tracks                        │  │
│  │ vehicles                      │  │
│  │ game_versions                 │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

## Datenfluss

### 1. CSV-Parsing
```
CSV → Parser → {
  race_number,
  race_date,
  track_name,
  fastest_lap_time,
  fastest_lap_driver,
  results[]
}
```

### 2. ID-Generierung
```
DB-Query → MAX(race_id) + 1 → race_id
DB-Query → MAX(grid_id) + 1 → grid_id_start
DB-Query → MAX(result_id) + 1 → result_id_start
```

### 3. Track-Mapping
```
"Blue Moon Bay - A" → TRACK_NAME_MAP → ("Blue Moon Bay Speedway", "Infield A") → tracks-Table → track_id = 6
```

### 4. Version-Ermittlung
```
race_date (2024-02-26) → game_versions WHERE release_date <= race_date → version_id = 97
```

### 5. Grid-Struktur
```
CSV Grid-Klassen: [1, 2a, 2b, 3]
  ↓
Grids-Tabelle:
  504 → Grid 1 (grid_number=1, grid_class=1)
  505 → Grid 2a (grid_number=2, grid_class=2a)
  506 → Grid 2b (grid_number=2, grid_class=2b)
  507 → Grid 3 (grid_number=3, grid_class=3)
```

### 6. Results-Berechnung
```
Für jeden Fahrer:
  - driver_id ← drivers-Table (PSN-Name)
  - team_id ← teams-Table (Team-Name normalized)
  - vehicle_id ← VEHICLE_MAP (Fahrzeug-Name)
  - grid_id ← grid_map[grid_class]
  - finish_pos_grid ← Position innerhalb Grid-Klasse
  - time_percent ← (race_seconds - penalty_seconds) / p1_time * 100
  - status ← 'DNF' if race_time = '8:00:00,000'
  - points_total ← parsed aus "Punkte"-Spalte
```

## Datenbank-Schema (Relevante Tabellen)

### races
```sql
race_id            INT PRIMARY KEY
season_id          INT FK → seasons
track_id           INT FK → tracks
version_id         INT FK → game_versions
race_date          DATE
fastest_lap_time   VARCHAR
fastest_lap_driver_id INT FK → drivers
```

### grids
```sql
grid_id            INT PRIMARY KEY
race_id            INT FK → races
grid_number        VARCHAR  -- '1', '2', '3'
grid_class         VARCHAR  -- '1', '2a', '2b', '3'
grid_label         VARCHAR  -- 'Grid 1', 'Grid 2a', etc.
```

### race_results
```sql
result_id          INT PRIMARY KEY
race_id            INT FK → races
grid_id            INT FK → grids
driver_id          INT FK → drivers
vehicle_id         INT FK → vehicles
team_id            INT FK → teams (NULL möglich)
start_pos_grid     INT (NULL - nicht im CSV)
finish_pos_grid    INT (berechnet)
finish_pos_overall INT (aus CSV)
race_time          VARCHAR (Format: 'H:MM:SS.mmm')
time_percent       DECIMAL
points_base        INT (= points_total, später manuell trennen)
points_bonus       INT (0, später manuell nachtragen)
points_total       INT
status             VARCHAR ('DNF' oder NULL)
penalty_seconds    INT
penalty_points     INT
```

## Fehlerbehandlung

### Auto-Recovery
- Neue Teams → Automatisch anlegen mit nächster team_id
- Neue Fahrer → Automatisch anlegen mit nächster driver_id
- Unbekannter Track → ValueError + Abbruch
- Unbekanntes Fahrzeug → Warning, Import fortsetzen

### Transaktionen
- Alle DB-Operationen in einer Transaktion
- Bei Fehler: Automatisches Rollback
- Commit nur nach erfolgreicher Validierung

### Validierung
```python
1. Result-Count prüfen
2. Grid-Verteilung prüfen
3. Keine NULL-Werte wo nicht erlaubt
4. Alle FKs existieren
```

## Konfiguration

### Environment-Variablen (.env)
```env
DB_HOST=localhost        # MySQL Host
DB_PORT=3306            # MySQL Port
DB_NAME=d046d457        # Datenbankname
DB_USER=user            # DB-User
DB_PASSWORD=pass        # DB-Passwort
SEASON_ID=12            # Saison-ID
```

### Track-Mapping (rtc_import.py)
```python
TRACK_NAME_MAP = {
    'CSV-Name': ('DB-Name', 'DB-Variant'),
}
```

### Vehicle-Mapping (rtc_import.py)
```python
VEHICLE_MAP = {
    'CSV-Fahrzeugname': vehicle_id,
}
```

### Team-Normalisierung (rtc_import.py)
```python
TEAM_NORMALIZATIONS = {
    'KotzBärTV': 'KOTZBÄR TV',
}
```

## Performance

- **Import-Zeit**: ~2-5 Sekunden pro Rennen
- **DB-Queries**: ~15-20 pro Import
- **Transaktions-Overhead**: Minimal (single commit)
- **Memory**: <50MB

## Sicherheit

- Credentials in .env (nicht in Git)
- SQL-Injection-geschützt (Prepared Statements)
- File-Permissions: 600 für .env
- Backup vor Batch-Import
- Read-Only User für Referenz-Queries möglich

## Erweiterbarkeit

### Neue Tracks hinzufügen
```python
TRACK_NAME_MAP['Neuer Track'] = ('DB Name', 'Variant')
```

### Neue Fahrzeuge hinzufügen
```python
VEHICLE_MAP['Neues Auto'] = 52  # vehicle_id
```

### Logging hinzufügen
```python
import logging
logging.basicConfig(
    filename=f'logs/import_{datetime.now()}.log',
    level=logging.INFO
)
```

### Web-Interface (zukünftig)
```python
from flask import Flask, request, jsonify

@app.route('/import', methods=['POST'])
def import_race():
    csv_file = request.files['csv']
    importer = RTCImporter(csv_file)
    importer.run()
    return jsonify({'status': 'success'})
```
