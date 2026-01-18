# Flight Importer

This project loads `flight_manifest.csv` into a Neo4j graph database using `flight_importer.py` every time the containerized import service runs.

## Quick start

```bash
docker compose up --build
```

The importer service waits for Neo4j to become available before streaming rows from the CSV file in batches of 4,000. Adjust `BATCH_SIZE`, `CSV_PATH`, and Neo4j credentials via environment variables in `docker-compose.yml` if needed.

## Filter `airports_mapping.csv` by ICAO codes

To remove rows from `airports_mapping.csv` whose `ICAO` code is *not* present in `unikalne_kody.csv` (column `airportCode`), use:

```bash
python filter_airports_mapping.py \
  --mapping airports_mapping1.csv \
  --allowed unikalne_kody.csv \
  --output airports_mapping.csv
```

To overwrite in place:

```bash
python filter_airports_mapping.py --in-place
```
