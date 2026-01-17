from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Iterable, List, Sequence

import folium
from PyQt6.QtCore import Qt, QUrl
from PyQt6.QtWidgets import (
    QApplication,
    QButtonGroup,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWebEngineCore import QWebEngineSettings

from smoska.shortest_path_distance import ShortestRoute, shortest_route
from smoska.shortest_path_time import FastestRoute, fastest_route


def _format_distance(result: ShortestRoute) -> str:
    lines: List[str] = [f"Total distance: {result.total_distance_nm:.1f} nm"]
    for idx, stop in enumerate(result.stops, start=1):
        lines.append(
            f"{idx}. {stop.code} - {stop.name or 'Unknown'} "
            f"(lat: {stop.lat if stop.lat is not None else 'n/a'}, "
            f"lon: {stop.lon if stop.lon is not None else 'n/a'})"
        )
    return "\n".join(lines)


def _format_time(result: FastestRoute) -> str:
    lines: List[str] = [f"Total time: {result.total_time_minutes:.1f} min ({result.total_time_minutes/60:.2f} h)"]
    for idx, stop in enumerate(result.stops, start=1):
        lines.append(
            f"{idx}. {stop.code} - {stop.name or 'Unknown'} "
            f"(lat: {stop.lat if stop.lat is not None else 'n/a'}, "
            f"lon: {stop.lon if stop.lon is not None else 'n/a'})"
        )
    return "\n".join(lines)


def _render_map(stops: Sequence) -> Path | None:
    coords = [(s.lat, s.lon, s.code) for s in stops if s.lat is not None and s.lon is not None]
    if len(coords) < 2:
        return None

    avg_lat = sum(lat for lat, _, _ in coords) / len(coords)
    avg_lon = sum(lon for _, lon, _ in coords) / len(coords)
    fmap = folium.Map(location=(avg_lat, avg_lon), zoom_start=4)

    folium.PolyLine([(lat, lon) for lat, lon, _ in coords], color="blue", weight=4, opacity=0.7).add_to(fmap)
    for lat, lon, code in coords:
        folium.Marker(location=(lat, lon), popup=code).add_to(fmap)

    tmp_dir = Path(tempfile.gettempdir())
    tmp_file = tmp_dir / "route_map.html"
    fmap.save(tmp_file)
    return tmp_file


class RouteApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Trasa lotnicza")
        self.resize(1100, 800)

        self.src_edit = QLineEdit()
        self.dst_edit = QLineEdit()

        self.mode_distance = QPushButton("Dystans")
        self.mode_time = QPushButton("Czas")
        for btn in (self.mode_distance, self.mode_time):
            btn.setCheckable(True)
        self.mode_distance.setChecked(True)
        mode_group = QButtonGroup(self)
        mode_group.addButton(self.mode_distance)
        mode_group.addButton(self.mode_time)

        self.fetch_btn = QPushButton("Pokaż trasę")
        self.map_btn = QPushButton("Pokaż mapę")

        self.output = QTextEdit()
        self.output.setReadOnly(True)
        self.map_view = QWebEngineView()
        self.map_view.settings().setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, True)

        self.last_stops: List | None = None

        self._layout()
        self.fetch_btn.clicked.connect(self.on_fetch)
        self.map_btn.clicked.connect(self.on_show_map)

    def _layout(self):
        top = QWidget()
        grid = QGridLayout(top)
        grid.addWidget(QLabel("Kod startowy:"), 0, 0)
        grid.addWidget(self.src_edit, 0, 1)
        grid.addWidget(QLabel("Kod docelowy:"), 1, 0)
        grid.addWidget(self.dst_edit, 1, 1)

        mode_layout = QHBoxLayout()
        mode_layout.addWidget(QLabel("Tryb trasy:"))
        mode_layout.addWidget(self.mode_distance)
        mode_layout.addWidget(self.mode_time)
        mode_layout.addStretch()
        grid.addLayout(mode_layout, 2, 0, 1, 2)

        btn_layout = QHBoxLayout()
        btn_layout.addWidget(self.fetch_btn)
        btn_layout.addWidget(self.map_btn)
        btn_layout.addStretch()
        grid.addLayout(btn_layout, 3, 0, 1, 2)

        main_layout = QVBoxLayout(self)
        main_layout.addWidget(top)

        # Split area: text output on top, map below
        main_layout.addWidget(QLabel("Wynik:"))
        main_layout.addWidget(self.output, stretch=1)
        main_layout.addWidget(QLabel("Mapa:"))
        main_layout.addWidget(self.map_view, stretch=3)

    def _current_mode(self) -> str:
        return "time" if self.mode_time.isChecked() else "distance"

    def on_fetch(self):
        src = self.src_edit.text().strip()
        dst = self.dst_edit.text().strip()
        if not src or not dst:
            QMessageBox.warning(self, "Brak danych", "Wpisz oba kody lotnisk")
            return

        try:
            if self._current_mode() == "time":
                result = fastest_route(src, dst)
                formatter = _format_time
            else:
                result = shortest_route(src, dst)
                formatter = _format_distance
        except ValueError as exc:
            QMessageBox.critical(self, "Błąd", str(exc))
            return
        except Exception as exc:
            QMessageBox.critical(self, "Błąd", f"Nie udało się pobrać trasy: {exc}")
            return

        if result is None:
            QMessageBox.information(self, "Brak trasy", "Nie znaleziono połączenia między lotniskami")
            return

        self.last_stops = list(result.stops)
        self.output.setPlainText(formatter(result))
        self._update_map()

    def on_show_map(self):
        if not self.last_stops:
            QMessageBox.information(self, "Brak danych", "Najpierw pobierz trasę")
            return
        self._update_map()

    def _update_map(self):
        if not self.last_stops:
            return
        html_path = _render_map(self.last_stops)
        if html_path is None:
            QMessageBox.information(self, "Brak danych", "Brak współrzędnych aby narysować trasę")
            return
        self.map_view.load(QUrl.fromLocalFile(str(html_path)))


def main():
    app = QApplication(sys.argv)
    window = RouteApp()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

