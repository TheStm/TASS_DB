import csv
import os
import time
from datetime import datetime
from math import ceil
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
CSV_PATH = os.getenv("CSV_PATH")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
BATCH_SIZE = max(1, int(os.getenv("BATCH_SIZE", "4000")))
MAX_CONNECT_ATTEMPTS = max(1, int(os.getenv("NEO4J_CONNECT_RETRIES", "15")))
CONNECT_DELAY = float(os.getenv("NEO4J_CONNECT_DELAY", "2"))
AIRPORTS_METADATA = Path(os.getenv("AIRPORTS_METADATA", "airports_mapping1.csv"))
CSV_POPULATION = Path(os.getenv("POPULATION_CSV", "population.csv"))

AIRPORT_METADATA = {}
MISSING_METADATA = set()


def create_schema(tx):
    tx.run("""
    CREATE CONSTRAINT airport_code IF NOT EXISTS
    FOR (a:Airport) REQUIRE a.code IS UNIQUE
    """)
    tx.run("""
    CREATE CONSTRAINT flight_id IF NOT EXISTS
    FOR (f:Flight) REQUIRE f.id IS UNIQUE
    """)
    tx.run("""
    CREATE CONSTRAINT airline_code IF NOT EXISTS
    FOR (al:Airline) REQUIRE al.code IS UNIQUE
    """)
    tx.run("""
    CREATE CONSTRAINT aircraft_type IF NOT EXISTS
    FOR (ac:AircraftType) REQUIRE ac.type IS UNIQUE
    """)
    tx.run("""
    CREATE CONSTRAINT day_date IF NOT EXISTS
    FOR (d:Day) REQUIRE d.date IS UNIQUE
    """)
    tx.run("""
    CREATE CONSTRAINT country_name IF NOT EXISTS
    FOR (c:Country) REQUIRE c.name IS UNIQUE
    """)
    tx.run("""
    CREATE CONSTRAINT city_name_country IF NOT EXISTS
    FOR (c:City) REQUIRE (c.name, c.countryName) IS UNIQUE
    """)
    tx.run("""
    CREATE CONSTRAINT country_stats_unique IF NOT EXISTS
    FOR (cs:CountryStats) REQUIRE (cs.countryName, cs.year) IS UNIQUE
    """)


def import_batch(tx, rows):
    tx.run("""
    UNWIND $rows AS row

    MERGE (dep:Airport {code: row.adep})
      ON CREATE SET dep.lat = row.adep_lat,
                    dep.lon = row.adep_lon,
                    dep.name = row.adep_name

    MERGE (arr:Airport {code: row.ades})
      ON CREATE SET arr.lat = row.ades_lat,
                    arr.lon = row.ades_lon,
                    arr.name = row.ades_name

    MERGE (airline:Airline {code: row.operator})
    MERGE (aircraft:AircraftType {type: row.ac_type})
    MERGE (day:Day {date: date(row.day)})

    MERGE (f:Flight {id: row.flight_id})
      SET f.offBlockTime = datetime(row.off_block),
          f.arrivalTime  = datetime(row.arrival),
          f.durationMin  = row.duration_min,
          f.distanceNm   = row.distance_nm

    MERGE (f)-[:DEPARTS_FROM]->(dep)
    MERGE (f)-[:ARRIVES_TO]->(arr)
    MERGE (f)-[:OPERATED_BY]->(airline)
    MERGE (f)-[:AIRCRAFT]->(aircraft)
    MERGE (f)-[:ON_DAY]->(day)

    FOREACH (_ IN CASE WHEN row.adep_city IS NOT NULL AND row.adep_country IS NOT NULL THEN [1] ELSE [] END |
      MERGE (dep_country:Country {name: row.adep_country})
      MERGE (dep_city:City {name: row.adep_city, countryName: row.adep_country})
      MERGE (dep_city)-[:IN_COUNTRY]->(dep_country)
      MERGE (dep)-[:IN_CITY]->(dep_city)
    )

    FOREACH (_ IN CASE WHEN row.ades_city IS NOT NULL AND row.ades_country IS NOT NULL THEN [1] ELSE [] END |
      MERGE (arr_country:Country {name: row.ades_country})
      MERGE (arr_city:City {name: row.ades_city, countryName: row.ades_country})
      MERGE (arr_city)-[:IN_COUNTRY]->(arr_country)
      MERGE (arr)-[:IN_CITY]->(arr_city)
    )
    """, rows=rows)


def import_population_batch(tx, rows):
    tx.run("""
    UNWIND $rows AS row
    MERGE (c:Country {name: row.country})
    MERGE (cs:CountryStats {countryName: row.country, year: row.year})
      SET cs.population = row.population
    MERGE (c)-[:HAS_STATS]->(cs)
    """, rows=rows)


def connect_with_retry():
    attempts = 0
    while True:
        try:
            driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
            driver.verify_connectivity()
            return driver
        except ServiceUnavailable as exc:
            attempts += 1
            if attempts >= MAX_CONNECT_ATTEMPTS:
                raise RuntimeError(
                    "Nie udało się połączyć z Neo4j po kilku próbach"
                ) from exc
            print(
                "Neo4j wciąż się uruchamia, czekam",
                f"({attempts}/{MAX_CONNECT_ATTEMPTS})",
            )
            time.sleep(CONNECT_DELAY)



def find_csv_sources():
    if CSV_PATH:
        candidate = Path(CSV_PATH)
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        if not candidate.exists():
            raise FileNotFoundError(f"plik {candidate} nie istnieje")
        return [candidate]

    if not DATA_DIR.exists():
        raise FileNotFoundError(f"katalog {DATA_DIR} nie odnaleziony")

    files = sorted(DATA_DIR.glob("*.csv*"))
    if not files:
        raise FileNotFoundError(f"brak plików CSV w katalogu {DATA_DIR}")

    return files


def load_airport_metadata():
    if not AIRPORTS_METADATA.exists():
        print(f"Brak pliku {AIRPORTS_METADATA}, pomijam mapowanie kodów lotnisk.")
        return {}

    metadata = {}
    with AIRPORTS_METADATA.open(newline="", encoding="utf-8") as fp:
        reader = csv.reader(fp)
        for row in reader:
            if len(row) < 5:
                continue
            icao = row[0].strip()
            city = row[2].strip()
            country = row[3].strip()
            iata = row[4].strip()
            airport_name = row[1].strip()  # Assuming the full airport name is in the second column
            if not city or not country:
                continue
            entry = {"name": airport_name, "city": city, "country": country}
            for raw_code in (icao, iata):
                normalized = raw_code.upper()
                if normalized and normalized not in metadata:
                    metadata[normalized] = entry
    print(f"Wczytano mapowanie {len(metadata)} kodów lotnisk.")
    return metadata


AIRPORT_METADATA = load_airport_metadata()


def resolve_airport_metadata(code):
    if not AIRPORT_METADATA or not code:
        return None
    normalized = code.strip().upper()
    if not normalized:
        return None
    metadata = AIRPORT_METADATA.get(normalized)
    if metadata:
        return metadata
    MISSING_METADATA.add(normalized)
    return None


def import_from_csv(session, csv_path):
    print(f"Czytam {csv_path.name}...")
    df = pd.read_csv(csv_path, compression="infer")

    mapping_df = pd.DataFrame.from_dict(AIRPORT_METADATA, orient="index")
    mapping_df.index.name = "code"
    mapping_df = mapping_df.reset_index()

    df["ADEP_norm"] = df["ADEP"].astype(str).str.strip().str.upper()
    df["ADES_norm"] = df["ADES"].astype(str).str.strip().str.upper()

    df = df.merge(
        mapping_df.add_prefix("adep_"),
        left_on="ADEP_norm",
        right_on="adep_code",
        how="left",
    ).merge(
        mapping_df.add_prefix("ades_"),
        left_on="ADES_norm",
        right_on="ades_code",
        how="left",
    )

    missing_mask = df["adep_country"].isna() | df["ades_country"].isna()
    if missing_mask.any():
        missing_codes = set(df.loc[missing_mask, "ADEP_norm"]).union(
            set(df.loc[missing_mask, "ADES_norm"])
        )
        missing_codes.discard(None)
        missing_codes.discard("")
        if missing_codes:
            MISSING_METADATA.update(missing_codes)
        print(
            f"Pomijam {missing_mask.sum()} lotów – brak mapowania dla: {', '.join(sorted(missing_codes))}"
        )
    df = df.loc[~missing_mask].copy()

    total_rows = len(df)
    if total_rows == 0:
        print("Brak lotów do zaimportowania po odfiltrowaniu brakujących mapowań.")
        return
    total_batches = ceil(total_rows / BATCH_SIZE)

    print(f"Załadowano {total_rows:,} wierszy (po odfiltrowaniu brakujących mapowań)")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Liczba batchy: {total_batches}")

    batch = []
    batch_no = 1

    for idx, r in df.iterrows():
        off_block = datetime.strptime(
            r["ACTUAL OFF BLOCK TIME"], "%d-%m-%Y %H:%M:%S"
        )
        arrival = datetime.strptime(
            r["ACTUAL ARRIVAL TIME"], "%d-%m-%Y %H:%M:%S"
        )

        batch.append({
            "flight_id": str(r["ECTRL ID"]),
            "adep": r["ADEP_norm"],
            "adep_lat": float(r["ADEP Latitude"]),
            "adep_lon": float(r["ADEP Longitude"]),
            "ades": r["ADES_norm"],
            "ades_lat": float(r["ADES Latitude"]),
            "ades_lon": float(r["ADES Longitude"]),
            "operator": r["AC Operator"],
            "ac_type": r["AC Type"],
            "off_block": off_block.isoformat(),
            "arrival": arrival.isoformat(),
            "day": off_block.date().isoformat(),
            "duration_min": int((arrival - off_block).total_seconds() / 60),
            "distance_nm": int(r["Actual Distance Flown (nm)"]),
            "adep_city": r["adep_city"],
            "adep_country": r["adep_country"],
            "adep_name": r["adep_name"],
            "ades_city": r["ades_city"],
            "ades_country": r["ades_country"],
            "ades_name": r["ades_name"],
        })

        if len(batch) >= BATCH_SIZE:
            print(f"Import batch {batch_no}/{total_batches}")
            session.execute_write(import_batch, batch)
            batch.clear()
            batch_no += 1

    if batch:
        print(f"Import batch {batch_no}/{total_batches}")
        session.execute_write(import_batch, batch)


def import_population(session):
    if not CSV_POPULATION.exists():
        print(f"Brak pliku {CSV_POPULATION}, pomijam import populacji.")
        return

    df = pd.read_csv(CSV_POPULATION, encoding="utf-8")
    df = df[df["TIME_PERIOD"].isin([2017, 2018])]
    df = df.rename(
        columns={
            "Geopolitical entity (reporting)": "country",
            "TIME_PERIOD": "year",
            "OBS_VALUE": "population",
        }
    )
    df = df[["country", "year", "population"]]
    df["country"] = df["country"].astype(str).str.strip()
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df = df.dropna(subset=["country", "population"])
    if df.empty:
        print("Brak danych populacji dla lat 2017/2018.")
        return

    rows = [
        {"country": r["country"], "year": int(r["year"]), "population": int(r["population"])}
        for r in df.to_dict(orient="records")
    ]

    total_rows = len(rows)
    total_batches = ceil(total_rows / BATCH_SIZE)

    for batch_no, start in enumerate(range(0, total_rows, BATCH_SIZE), start=1):
        batch = rows[start:start + BATCH_SIZE]
        print(f"Import population batch {batch_no}/{total_batches}")
        session.execute_write(import_population_batch, batch)


def main():
    driver = connect_with_retry()

    with driver.session() as session:
        print("Tworzę schemat...")
        session.execute_write(create_schema)

        csv_files = find_csv_sources()
        for csv_file in csv_files:
            import_from_csv(session, csv_file)

        import_population(session)

    driver.close()

    if MISSING_METADATA:
        sample = ", ".join(sorted(MISSING_METADATA))
        print(f"Nie znaleziono mapowania dla kodów: {sample}")
    print("IMPORT ZAKOŃCZONY SUKCESEM")



if __name__ == "__main__":
    main()
