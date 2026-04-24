# 🚀 Quick Start Guide

## 5-Minuten-Setup

### 1. Environment konfigurieren (2 Min)

```bash
cd ~/rtc-import

# Environment-Template kopieren
cp .env.example .env

# Mit echten DB-Daten befüllen
nano .env
```

Trage deine DB-Credentials ein:
```env
DB_HOST=localhost
DB_NAME=d046d457
DB_USER=dein_db_user
DB_PASSWORD=dein_passwort
SEASON_ID=12
```

### 2. Dependencies installieren (1 Min)

```bash
pip3 install -r requirements.txt
```

### 3. Environment aktivieren (30 Sek)

**Für diesen Session:**
```bash
export $(cat .env | xargs)
```

**Permanent (empfohlen):**
```bash
echo 'export $(cat ~/rtc-import/.env | xargs)' >> ~/.bashrc
source ~/.bashrc
```

### 4. Ersten Import testen (1 Min)

```bash
# Test-CSV in data/ kopieren
cp /pfad/zu/rennen5.csv data/

# Import starten
python3 rtc_import.py data/rennen5.csv
```

### 5. Fertig! ✓

```
============================================================
RTC Rennen Import - Production
============================================================
✓ DB-Verbindung hergestellt (Season 12)

Lade Referenzdaten...
  ✓ 531 Fahrer
  ✓ 108 Teams
  ✓ 101 Tracks
  ✓ 59 GT7 Versionen

...

============================================================
✓ IMPORT ERFOLGREICH ABGESCHLOSSEN
============================================================
```

## Nächste Schritte

### Batch-Import aller Rennen

```bash
# Alle CSVs in data/ kopieren
cp /pfad/zu/rennen*.csv data/

# Backup erstellen
./scripts/backup_db.sh

# Alle importieren
./scripts/import_all.sh
```

### Bonus-Punkte nachtragen

```sql
-- Schnellste Runde (Beispiel)
UPDATE race_results 
SET points_bonus = 3, points_total = points_base + 3
WHERE race_id = 216 AND driver_id = 1035;
```

## Häufige Probleme

### "DB-Verbindung fehlgeschlagen"
```bash
# Environment prüfen
echo $DB_HOST $DB_NAME

# DB-Verbindung testen
mysql -u $DB_USER -p -h $DB_HOST $DB_NAME
```

### "Track nicht gefunden"
→ In `rtc_import.py` das `TRACK_NAME_MAP` erweitern (Zeile ~70)

### "Fahrzeug nicht gefunden"
→ In `rtc_import.py` das `VEHICLE_MAP` erweitern (Zeile ~30)

## Support

Vollständige Doku: [README.md](README.md)
