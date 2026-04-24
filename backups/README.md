# Backups Directory

DB-Backups werden hier gespeichert.

## Automatisches Backup

Vor Batch-Import:
```bash
./scripts/backup_db.sh
```

## Manuelles Backup

```bash
mysqldump -u user -p d046d457 > backups/manual_backup_$(date +%Y%m%d_%H%M%S).sql
```

## Restore aus Backup

```bash
mysql -u user -p d046d457 < backups/rtc_backup_20240424_123456.sql
```

## Automatische Bereinigung

Das `backup_db.sh` Script löscht automatisch Backups die älter als 30 Tage sind.
