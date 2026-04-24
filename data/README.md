# Data Directory

Lege hier deine CSV-Dateien ab:

```bash
cp /pfad/zu/rennen5.csv data/
cp /pfad/zu/rennen6.csv data/
# etc.
```

## CSV-Dateinamen

Empfohlene Benennung:
- `rennen1.csv`
- `rennen2.csv`
- `rennen3.csv`
- etc.

Oder mit Datum:
- `2024-01-29_rennen1.csv`
- `2024-02-12_rennen2.csv`
- etc.

## .gitignore

CSV-Dateien werden standardmäßig NICHT ins Git-Repo übernommen (siehe `.gitignore`).
Falls du sie doch tracken willst, kommentiere in `.gitignore` aus:

```gitignore
# data/*.csv  # <-- auskommentieren
```
