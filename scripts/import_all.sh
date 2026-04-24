#!/bin/bash
# RTC Import - Batch Import aller Rennen

set -e

# Lade Environment
if [ -f .env ]; then
    export $(cat .env | xargs)
else
    echo "✗ .env nicht gefunden!"
    exit 1
fi

# Farben
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "============================================"
echo "RTC Rennen Batch-Import"
echo "============================================"
echo ""

# Backup erstellen
echo "📦 Erstelle Backup..."
./scripts/backup_db.sh
echo ""

# Zähle CSVs
CSV_COUNT=$(ls data/*.csv 2>/dev/null | wc -l)

if [ $CSV_COUNT -eq 0 ]; then
    echo -e "${RED}✗ Keine CSV-Dateien in data/ gefunden!${NC}"
    exit 1
fi

echo "📊 Gefunden: $CSV_COUNT Rennen"
echo ""

# Import-Zähler
SUCCESS=0
FAILED=0

# Importiere alle CSVs
for csv in data/*.csv; do
    BASENAME=$(basename "$csv")
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📥 Importiere: $BASENAME"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if python3 rtc_import.py "$csv"; then
        echo -e "${GREEN}✓ $BASENAME erfolgreich importiert${NC}"
        ((SUCCESS++))
    else
        echo -e "${RED}✗ Fehler bei $BASENAME${NC}"
        ((FAILED++))
        
        # Bei Fehler: Abbrechen oder weitermachen?
        read -p "Trotzdem fortfahren? (j/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Jj]$ ]]; then
            echo -e "${YELLOW}⚠️  Import abgebrochen${NC}"
            exit 1
        fi
    fi
    
    echo ""
done

# Zusammenfassung
echo "============================================"
echo "Import abgeschlossen"
echo "============================================"
echo -e "${GREEN}✓ Erfolgreich: $SUCCESS${NC}"
if [ $FAILED -gt 0 ]; then
    echo -e "${RED}✗ Fehlgeschlagen: $FAILED${NC}"
fi
echo ""

# Validierung
echo "📊 Validiere Datenbank..."
mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p$DB_PASSWORD $DB_NAME <<EOF
SELECT 
    s.name AS Saison,
    COUNT(r.race_id) AS Rennen,
    COUNT(DISTINCT rr.driver_id) AS Fahrer,
    SUM(CASE WHEN rr.race_id IS NOT NULL THEN 1 ELSE 0 END) AS Ergebnisse
FROM seasons s
LEFT JOIN races r ON s.season_id = r.season_id
LEFT JOIN race_results rr ON r.race_id = rr.race_id
WHERE s.season_id = $SEASON_ID
GROUP BY s.season_id;
EOF

echo ""
echo -e "${GREEN}✓ Batch-Import abgeschlossen!${NC}"
