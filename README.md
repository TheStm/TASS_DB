# Flight Importer

This project loads `flight_manifest.csv` into a Neo4j graph database using `flight_importer.py` every time the containerized import service runs.

## Quick start

```bash
docker compose up --build
```

The importer service waits for Neo4j to become available before streaming rows from the CSV file in batches of 4,000. Adjust `BATCH_SIZE`, `CSV_PATH`, and Neo4j credentials via environment variables in `docker-compose.yml` if needed.

