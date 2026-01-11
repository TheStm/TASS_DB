import os
import time
from datetime import datetime
from math import ceil
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


# ============================================================
# KONFIGURACJA
# ============================================================

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
CSV_PATH = os.getenv("CSV_PATH")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
BATCH_SIZE = max(1, int(os.getenv("BATCH_SIZE", "4000")))
MAX_CONNECT_ATTEMPTS = max(1, int(os.getenv("NEO4J_CONNECT_RETRIES", "15")))
CONNECT_DELAY = float(os.getenv("NEO4J_CONNECT_DELAY", "2"))


# ============================================================
# SCHEMA (uruchomi się tylko raz dzięki IF NOT EXISTS)
# ============================================================

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


def import_batch(tx, rows):
    tx.run("""
    UNWIND $rows AS row

    MERGE (dep:Airport {code: row.adep})
      ON CREATE SET dep.lat = row.adep_lat,
                    dep.lon = row.adep_lon

    MERGE (arr:Airport {code: row.ades})
      ON CREATE SET arr.lat = row.ades_lat,
                    arr.lon = row.ades_lon

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



# ============================================================
# HELPERS
# ============================================================

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


def import_from_csv(session, csv_path):
    print(f"Czytam {csv_path.name}...")
    df = pd.read_csv(csv_path, compression="infer")
    total_rows = len(df)
    total_batches = ceil(total_rows / BATCH_SIZE)

    print(f"Załadowano {total_rows:,} wierszy")
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
            "adep": r["ADEP"],
            "adep_lat": float(r["ADEP Latitude"]),
            "adep_lon": float(r["ADEP Longitude"]),
            "ades": r["ADES"],
            "ades_lat": float(r["ADES Latitude"]),
            "ades_lon": float(r["ADES Longitude"]),
            "operator": r["AC Operator"],
            "ac_type": r["AC Type"],
            "off_block": off_block.isoformat(),
            "arrival": arrival.isoformat(),
            "day": off_block.date().isoformat(),
            "duration_min": int((arrival - off_block).total_seconds() / 60),
            "distance_nm": int(r["Actual Distance Flown (nm)"])
        })

        if len(batch) >= BATCH_SIZE:
            print(f"Import batch {batch_no}/{total_batches}")
            session.execute_write(import_batch, batch)
            batch.clear()
            batch_no += 1

    if batch:
        print(f"Import batch {batch_no}/{total_batches}")
        session.execute_write(import_batch, batch)



def main():
    driver = connect_with_retry()

    with driver.session() as session:
        print("Tworzę schemat...")
        session.execute_write(create_schema)

        csv_files = find_csv_sources()
        for csv_file in csv_files:
            import_from_csv(session, csv_file)

    driver.close()
    print("IMPORT ZAKOŃCZONY SUKCESEM")



if __name__ == "__main__":
    main()
