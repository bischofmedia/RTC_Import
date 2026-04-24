# Logs Directory

Hier werden automatisch Import-Logs gespeichert (zukünftige Feature).

Aktuell: Logs werden auf STDOUT ausgegeben.

Um Logs zu speichern:

```bash
python3 rtc_import.py data/rennen5.csv 2>&1 | tee logs/import_$(date +%Y%m%d_%H%M%S).log
```
