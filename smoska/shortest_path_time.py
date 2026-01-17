"""Helpers to build a GDS projection and fetch the fastest route between airports."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
GRAPH_NAME = os.getenv("GDS_GRAPH_NAME_TIME", "airportFastest")
MAX_CONNECT_ATTEMPTS = max(1, int(os.getenv("NEO4J_CONNECT_RETRIES", "5")))
CONNECT_DELAY = float(os.getenv("NEO4J_CONNECT_DELAY", "2"))
NEO4J_SUPPRESS_WARNINGS = os.getenv("NEO4J_SUPPRESS_WARNINGS", "1") == "1"

if NEO4J_SUPPRESS_WARNINGS:
    logging.getLogger("neo4j").setLevel(logging.ERROR)

# Cypher snippets reused across calls
_GRAPH_EXISTS = "CALL gds.graph.exists($graph_name) YIELD exists"
_DROP_GRAPH = "CALL gds.graph.drop($graph_name, false)"
_PROJECT_GRAPH = """
CALL gds.graph.project.cypher(
  $graph_name,
  'MATCH (a:Airport) RETURN id(a) AS id',
  'MATCH (a:Airport)<-[:DEPARTS_FROM]-(f:Flight)-[:ARRIVES_TO]->(b:Airport)\nWHERE f.durationMin IS NOT NULL AND f.durationMin > 0\nRETURN id(a) AS source, id(b) AS target, min(f.durationMin) AS time\nUNION\nMATCH (a:Airport)<-[:DEPARTS_FROM]-(f:Flight)-[:ARRIVES_TO]->(b:Airport)\nWHERE f.durationMin IS NOT NULL AND f.durationMin > 0\nRETURN id(b) AS source, id(a) AS target, min(f.durationMin) AS time'
)
"""
_NODE_IDS = """
MATCH (source:Airport {code: $source}), (target:Airport {code: $target})
RETURN id(source) AS sourceId, id(target) AS targetId
"""
_SHORTEST_PATH = """
CALL gds.shortestPath.dijkstra.stream(
  $graph_name,
  {
    sourceNode: $sourceId,
    targetNode: $targetId,
    relationshipWeightProperty: 'time'
  }
)
YIELD nodeIds, totalCost
RETURN [nid IN nodeIds | {code: gds.util.asNode(nid).code, name: gds.util.asNode(nid).name, lat: gds.util.asNode(nid).lat, lon: gds.util.asNode(nid).lon}] AS route,
       totalCost AS totalTimeMinutes
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


def _ensure_graph(session, graph_name: str, rebuild: bool) -> None:
    exists_rec = session.run(_GRAPH_EXISTS, graph_name=graph_name).single()
    exists = bool(exists_rec and exists_rec.get("exists"))
    if rebuild or not exists:
        session.run(_DROP_GRAPH, graph_name=graph_name)
        session.run(_PROJECT_GRAPH, graph_name=graph_name)


@dataclass
class AirportStop:
    code: str
    name: str | None = None
    lat: float | None = None
    lon: float | None = None

    def to_dict(self) -> Dict[str, Optional[str | float]]:
        return {"code": self.code, "name": self.name, "lat": self.lat, "lon": self.lon}


@dataclass
class FastestRoute:
    stops: List[AirportStop]
    total_time_minutes: float

    def to_dict(self) -> Dict[str, object]:
        return {
            "route": [stop.to_dict() for stop in self.stops],
            "totalTimeMinutes": self.total_time_minutes,
            "totalTimeHours": round(self.total_time_minutes / 60.0, 2),
        }


def fastest_route(
    source_code: str,
    target_code: str,
    *,
    graph_name: str = GRAPH_NAME,
    rebuild_graph: bool = False,
    driver=None,
) -> Optional[FastestRoute]:
    """Return fastest route and total time (minutes) between two airport codes."""

    source_code = (source_code or "").strip().upper()
    target_code = (target_code or "").strip().upper()
    if not source_code or not target_code:
        raise ValueError("Both source_code and target_code are required")

    created_driver = driver is None
    neo4j_driver = driver or _connect_with_retry()

    try:
        with neo4j_driver.session() as session:
            _ensure_graph(session, graph_name, rebuild_graph)

            ids = session.run(_NODE_IDS, source=source_code, target=target_code).single()
            if not ids:
                raise ValueError("Unknown airport code provided")

            record = session.run(
                _SHORTEST_PATH,
                graph_name=graph_name,
                sourceId=ids["sourceId"],
                targetId=ids["targetId"],
            ).single()
            if not record:
                return None

            stops = [
                AirportStop(code=n["code"], name=n.get("name"), lat=n.get("lat"), lon=n.get("lon"))
                for n in record["route"]
            ]
            return FastestRoute(stops=stops, total_time_minutes=record["totalTimeMinutes"])
    finally:
        if created_driver:
            neo4j_driver.close()


if __name__ == "__main__":
    # Example manual run: python smoska/shortest_path_time.py EPWA LPPT
    import sys

    if len(sys.argv) != 3:
        print("Usage: python smoska/shortest_path_time.py <SRC_CODE> <DST_CODE>")
        raise SystemExit(1)

    result = fastest_route(sys.argv[1], sys.argv[2])
    if result is None:
        print("Brak trasy między podanymi lotniskami")
    else:
        print(result.to_dict())

