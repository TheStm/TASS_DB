# Flight Importer

This project loads `flight_manifest.csv` into a Neo4j graph database using `flight_importer.py` every time the containerized import service runs.

## Quick start (Neo4j + import danych)

```bash
docker compose up --build
```

The importer service waits for Neo4j to become available before streaming rows from the CSV file in batches of 4,000. Adjust `BATCH_SIZE`, `CSV_PATH`, and Neo4j credentials via environment variables in `docker-compose.yml` if needed.

## Aplikacja GUI (`gui_app_qt.py`)

`gui_app_qt.py` to desktopowa aplikacja (PySide6/Qt) do analizy połączeń lotniczych zapisanych w bazie Neo4j oraz do przeglądania gotowych raportów CSV.

### Dostępne moduły (zakładki)

- **Najkrótsza trasa** – wyszukiwanie trasy pomiędzy lotniskami (kody ICAO) z optymalizacją:
  - *Najkrótszy dystans* (NM / km) albo
  - *Najszybszy lot* (min / h).
  Wynik jest prezentowany jako lista przystanków oraz (opcjonalnie) mapa z przebiegiem trasy.

- **Analiza hubów** – wykrywanie najważniejszych lotnisk-hubów na podstawie zapytania do Neo4j.
  Wyniki są prezentowane w tabeli (m.in. liczba operacji, kierunków, hub score) oraz na mapie.

- **Statystyki popularności** – widok oparty o pliki w `reports/` (raporty roczne i miesięczne) + `population.csv`.
  Pokazuje m.in. top destynacji i tras oraz sezonowość (wykres kwartalny: 03/06/09/12) dla wybranego kraju.

### Wymagania

- Python 3.12
- Zainstalowane zależności z `requirements.txt`
- Dla zakładek korzystających z bazy: uruchomiony Neo4j z zaimportowanymi danymi (najprościej przez `docker compose up --build`)
- (Opcjonalnie) **QtWebEngine** do map. Jeżeli `PySide6-QtWebEngine` nie jest dostępny, aplikacja nadal działa, ale zamiast map pokaże komunikat.

### Uruchomienie

1) Postaw Neo4j i wykonaj import (jednorazowo lub gdy zmieniasz dane):

```bash
docker compose up --build
```

2) Utwórz środowisko i zainstaluj zależności (lokalnie):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3) Uruchom GUI:

```bash
python gui_app_qt.py
```

### Konfiguracja połączenia z Neo4j

Domyślnie kontener Neo4j wystawia port `7687` (bolt) oraz `7474` (UI). Skrypty analityczne w projekcie zwykle zakładają:

- URI: `bolt://localhost:7687`
- user: `neo4j`
- hasło: `password`

Jeśli zmienisz je w `docker-compose.yml`, upewnij się, że te same dane są ustawione w modułach zapytań (np. `hub_analysis.py`, `shortest_path_distance.py`, `shortest_path_time.py`).

### Dane wejściowe

- Lista lotnisk jest ładowana z `airports_mapping.csv`.
- Raporty dla zakładki statystyk są czytane z `reports/` oraz `population.csv`.
