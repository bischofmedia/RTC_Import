# RTC Rennen Import System

Automatisiertes Import-System für RTC GT7-Serie Rennergebnisse.

## 📁 Projektstruktur

```
rtc-import/
├── rtc_import.py          # Haupt-Import-Script
├── .env.example           # Environment-Template
├── .env                   # Deine DB-Credentials (nicht in Git!)
├── .gitignore            # Git-Ignore-Regeln
├── README.md             # Diese Datei
├── requirements.txt      # Python-Dependencies
├── data/                 # CSV-Dateien hier ablegen
├── logs/                 # Import-Logs (automatisch)
├── backups/              # DB-Backups vor Import
└── docs/                 # Zusätzliche Dokumentation
```

## 🚀 Installation

### 1. Python-Dependencies installieren

```bash
pip3 install -r requirements.txt
```

### 2. Environment konfigurieren

```bash
# Template kopieren
cp .env.example .env

# Mit deinen Daten befüllen
nano .env
```

Trage ein:
```env
DB_HOST=localhost
DB_PORT=3306
DB_NAME=d046d457
DB_USER=dein_user
DB_PASSWORD=dein_passwort
SEASON_ID=12
```

### 3. Environment aktivieren

**Option A: Manuell** (vor jedem Import)
```bash
export $(cat .env | xargs)
```

**Option B: In ~/.bashrc** (empfohlen)
```bash
echo 'export $(cat ~/rtc-import/.env | xargs)' >> ~/.bashrc
source ~/.bashrc
```

**Option C: Mit python-dotenv** (automatisch)
```bash
pip3 install python-dotenv
# Script lädt .env automatisch
```

## 📥 Verwendung

### Einzelnes Rennen importieren

```bash
# CSV in data/ ablegen
cp rennen5.csv data/

# Environment laden (falls nicht in .bashrc)
export $(cat .env | xargs)

# Import starten
python3 rtc_import.py data/rennen5.csv
```

### Batch-Import aller Rennen

```bash
# Alle CSVs in data/ ablegen, dann:
chmod +x scripts/import_all.sh
./scripts/import_all.sh
```

## 📊 CSV-Format

Das Script erwartet CSVs im RTC-Standard-Format:

- **Zeile 2**: Rennen-Nummer in Spalte 2
- **Zeile 2**: Track-Name in Spalte 5
- **Zeile 3**: Datum in Spalte 5 (Format: `DD.MM.YYYY`)
- **Zeile 3**: Schnellste Runde Zeit in Spalte 7
- **Zeile 3**: Schnellste Runde Fahrer in Spalte 9
- **Header**: Zeile mit "Pos" in Spalte 2
- **Daten**: Ab Header+1

## 🔧 Track-Mapping erweitern

Falls ein Track nicht erkannt wird:

```python
# In rtc_import.py, Zeile ~70:
TRACK_NAME_MAP = {
    'Blue Moon Bay - A': ('Blue Moon Bay Speedway', 'Infield A'),
    'Dein Track': ('DB Track Name', 'DB Variant'),
}
```

## 🚗 Neues Fahrzeug hinzufügen

```python
# In rtc_import.py, Zeile ~30:
VEHICLE_MAP = {
    'McLaren 650S': 26,
    'Neues Auto': 52,  # vehicle_id aus DB
}
```

## 🏁 Team-Normalisierung

```python
# In rtc_import.py, Zeile ~95:
TEAM_NORMALIZATIONS = {
    'KotzBärTV': 'KOTZBÄR TV',
    'Dein Team': 'Normalisierter Name',
}
```

## 📝 Nach dem Import prüfen

### Bonus-Punkte nachtragen

```sql
-- Schnellste Runde (3 Bonuspunkte)
UPDATE race_results 
SET points_bonus = 3, points_total = points_base + 3
WHERE race_id = 216 AND driver_id = 1035;

-- Podium (1 Bonuspunkt)
UPDATE race_results 
SET points_bonus = 1, points_total = points_base + 1
WHERE race_id = 216 AND finish_pos_overall <= 3;
```

### Validierung

```sql
-- Ergebnisse zählen
SELECT COUNT(*) FROM race_results WHERE race_id = 216;

-- Grid-Verteilung
SELECT g.grid_class, COUNT(*) 
FROM grids g 
JOIN race_results r ON g.grid_id = r.grid_id 
WHERE race_id = 216 
GROUP BY g.grid_class;

-- Punkte-Summe prüfen
SELECT SUM(points_total) FROM race_results WHERE race_id = 216;
```

## 🔒 Sicherheit

- ✅ `.env` ist in `.gitignore`
- ✅ Niemals DB-Credentials committen!
- ✅ Vor Import: DB-Backup erstellen
- ✅ File-Permissions: `chmod 600 .env`

## 📦 Backup vor Import

```bash
# Automatisches Backup-Script
./scripts/backup_db.sh

# Oder manuell
mysqldump -u user -p d046d457 > backups/backup_$(date +%Y%m%d_%H%M%S).sql
```

## 🐛 Troubleshooting

### DB-Verbindung fehlgeschlagen
```bash
# Teste Verbindung
mysql -u $DB_USER -p$DB_PASSWORD -h $DB_HOST $DB_NAME

# Prüfe Environment
echo $DB_HOST $DB_NAME $DB_USER
```

### Track nicht gefunden
- Track-Name im CSV prüfen (Zeile 2, Spalte 5)
- `TRACK_NAME_MAP` erweitern
- Tracks in DB checken: `SELECT * FROM tracks WHERE name LIKE '%Blue%';`

### Fahrzeug nicht gefunden
- Fahrzeug-Name im CSV prüfen
- `VEHICLE_MAP` erweitern
- Vehicles in DB: `SELECT * FROM vehicles;`

## 📚 Dokumentation

- [Import-Workflow](docs/workflow.md)
- [Datenbank-Schema](docs/schema.md)
- [CSV-Format-Spec](docs/csv_format.md)

## 🤝 Support

Bei Problemen:
1. Log-Output prüfen
2. Traceback analysieren
3. DB-Logs: `/var/log/mysql/error.log`

## 📜 Lizenz

Internes Tool für RTC GT7-Serie
