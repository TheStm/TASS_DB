"""Utilities to fetch hub airports ranking from Neo4j."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import List, Optional

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
MAX_CONNECT_ATTEMPTS = max(1, int(os.getenv("NEO4J_CONNECT_RETRIES", "5")))
CONNECT_DELAY = float(os.getenv("NEO4J_CONNECT_DELAY", "2"))
NEO4J_SUPPRESS_WARNINGS = os.getenv("NEO4J_SUPPRESS_WARNINGS", "1") == "1"

if NEO4J_SUPPRESS_WARNINGS:
    logging.getLogger("neo4j").setLevel(logging.ERROR)


@dataclass
class HubAirport:
    code: str
    airport: Optional[str]
    country: Optional[str]
    total_ops: int
    unique_routes: int
    hub_score: float
    dominant_airline: Optional[str]
    airline_share_pct: Optional[float]


_CYPHER_HUBS = """
MATCH (a:Airport)
WHERE count { (a)--() } > $min_degree
OPTIONAL MATCH (a)-[:IN_CITY]->(city:City)
WITH a, city.countryName AS Country, count { (a)--() } AS total_ops
MATCH (a)<-[:DEPARTS_FROM]-(:Flight)-[:ARRIVES_TO]->(dest:Airport)
WITH a, Country, total_ops, count(DISTINCT dest) AS unique_routes
MATCH (a)<-[:DEPARTS_FROM]-(f:Flight)-[:OPERATED_BY]->(airline:Airline)
WITH a, Country, total_ops, unique_routes, airline, count(f) AS airline_flights
ORDER BY airline_flights DESC
WITH a, Country, total_ops, unique_routes, collect(airline.code)[0] AS top_airline, max(airline_flights) AS top_airline_count
RETURN 
    a.code AS code,
    a.name AS airport,
    Country AS country,
    total_ops AS total_ops,
    unique_routes AS unique_routes,
    (total_ops * unique_routes) AS hub_score,
    top_airline AS top_airline,
    round((toFloat(top_airline_count) / (total_ops/2)) * 100, 1) AS airline_share_pct
ORDER BY hub_score DESC
LIMIT $limit
"""


def _connect_with_retry():
    attempts = 0
    while True:
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            driver.verify_connectivity()
            return driver
        except ServiceUnavailable as exc:
            attempts += 1
            if attempts >= MAX_CONNECT_ATTEMPTS:
                raise RuntimeError("Failed to connect to Neo4j after retries") from exc
            time.sleep(CONNECT_DELAY)


def fetch_hubs(limit: int = 15, min_degree: int = 5000, driver=None) -> List[HubAirport]:
    """Return ranked hub airports using the provided Cypher query."""

    limit = max(1, int(limit))
    min_degree = max(0, int(min_degree))

    created_driver = driver is None
    neo4j_driver = driver or _connect_with_retry()

    try:
        with neo4j_driver.session() as session:
            records = session.run(
                _CYPHER_HUBS,
                limit=limit,
                min_degree=min_degree,
            )
            result: List[HubAirport] = []
            for rec in records:
                result.append(
                    HubAirport(
                        code=rec.get("code"),
                        airport=rec.get("airport"),
                        country=rec.get("country"),
                        total_ops=int(rec.get("total_ops", 0) or 0),
                        unique_routes=int(rec.get("unique_routes", 0) or 0),
                        hub_score=float(rec.get("hub_score", 0.0) or 0.0),
                        dominant_airline=rec.get("top_airline"),
                        airline_share_pct=rec.get("airline_share_pct"),
                    )
                )
            return result
    finally:
        if created_driver:
            neo4j_driver.close()

