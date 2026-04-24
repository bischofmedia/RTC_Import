#!/bin/bash
# RTC Import - DB Backup Script

set -e

# Lade Environment
if [ -f .env ]; then
    export $(cat .env | xargs)
else
    echo "✗ .env nicht gefunden!"
    exit 1
fi

# Backup-Dateiname mit Timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="backups/rtc_backup_${TIMESTAMP}.sql"

echo "📦 Erstelle DB-Backup..."
echo "  Database: $DB_NAME"
echo "  File: $BACKUP_FILE"

# Backup erstellen
mysqldump -h $DB_HOST -P $DB_PORT -u $DB_USER -p$DB_PASSWORD $DB_NAME > $BACKUP_FILE

# Prüfen
if [ -f $BACKUP_FILE ]; then
    SIZE=$(du -h $BACKUP_FILE | cut -f1)
    echo "✓ Backup erstellt: $SIZE"
    
    # Alte Backups löschen (älter als 30 Tage)
    find backups/ -name "*.sql" -mtime +30 -delete
    echo "✓ Alte Backups bereinigt (>30 Tage)"
else
    echo "✗ Backup fehlgeschlagen!"
    exit 1
fi
