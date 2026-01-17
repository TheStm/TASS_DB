"""Desktop GUI for analyzing flight connections with modular tabs."""

from __future__ import annotations

import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

import folium
import pandas as pd
from PySide6.QtCore import Qt, QUrl, Signal
from PySide6.QtGui import QCloseEvent, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QComboBox,
    QTableWidget,
    QTableWidgetItem,
)

try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWebEngineCore import QWebEngineSettings
except Exception:  # pragma: no cover - optional dependency for lightweight runs
    QWebEngineView = None  # type: ignore[assignment]
    QWebEngineSettings = None  # type: ignore[assignment]

from smoska import shortest_path_distance, shortest_path_time
import hub_analysis


@dataclass
class ModuleInfo:
    """Metadata describing an application module."""

    name: str
    description: str
    factory: Callable[["ApplicationContext"], QWidget]


@dataclass
class AirportRecord:
    code: str
    name: str
    city: str
    country: str
    lat: Optional[float]
    lon: Optional[float]

    @property
    def label(self) -> str:
        location = ", ".join(filter(None, [self.city, self.country]))
        return f"{self.code} — {self.name} ({location})"


class DataRepository:
    """Centralised loader for static data shared by all modules."""

    def __init__(self, airports_path: Path) -> None:
        self.airports_path = airports_path
        self._airports = self._load_airports()

    def _load_airports(self) -> List[AirportRecord]:
        if not self.airports_path.exists():
            raise FileNotFoundError(
                f"Nie znaleziono pliku lotnisk: {self.airports_path}"
            )

        df = pd.read_csv(self.airports_path)
        df = df[df["ICAO"].notna()].copy()
        df["ICAO"] = df["ICAO"].str.upper()
        df = df[df["ICAO"].str.len() == 4]
        df = df.sort_values("ICAO")

        def _text(val: object) -> str:
            return "" if pd.isna(val) else str(val)

        airports: List[AirportRecord] = []
        for _, row in df.iterrows():
            airports.append(
                AirportRecord(
                    code=row["ICAO"],
                    name=_text(row.get("Name")),
                    city=_text(row.get("City")),
                    country=_text(row.get("Country")),
                    lat=float(row["Latitude"]) if not pd.isna(row.get("Latitude")) else None,
                    lon=float(row["Longitude"]) if not pd.isna(row.get("Longitude")) else None,
                )
            )
        print(f"Załadowano {airports[0]}")
        return airports

    @property
    def airports(self) -> List[AirportRecord]:
        return self._airports

    def find_airport(self, code: str) -> Optional[AirportRecord]:
        code = (code or "").strip().upper()
        for airport in self._airports:
            if airport.code == code:
                return airport
        return None


class ApplicationContext:
    """Shared context that gives modules access to cached data."""

    def __init__(self, data_repo: DataRepository) -> None:
        self.data = data_repo


class ModuleCard(QFrame):
    """Simple card-like widget representing a module on the start screen."""

    launch_requested = Signal(ModuleInfo)

    def __init__(self, module: ModuleInfo, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._module = module
        self.setFrameShape(QFrame.StyledPanel)
        self.setObjectName("moduleCard")

        layout = QHBoxLayout(self)
        text_layout = QVBoxLayout()
        title = QLabel(f"<b>{module.name}</b>")
        desc = QLabel(module.description)
        desc.setWordWrap(True)
        text_layout.addWidget(title)
        text_layout.addWidget(desc)
        text_layout.addStretch(1)

        launch_button = QPushButton("Uruchom")
        launch_button.clicked.connect(lambda: self.launch_requested.emit(self._module))
        launch_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

        layout.addLayout(text_layout)
        layout.addWidget(launch_button)


class StartScreenWidget(QWidget):
    """Displays the list of available modules."""

    module_selected = Signal(ModuleInfo)

    def __init__(self, modules: List[ModuleInfo], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._modules = modules
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        intro = QLabel(
            "<h2>Analiza popularności miejsc turystycznych</h2>"
            "<p>Analiza popularności miejsc turystycznych na podstawie danych o lotach.</p>"
            "<p><b>Autorzy:</b> Zuzanna Popławska, Stanisław Moska, Filip Misztal</p>"
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)
        layout.addStretch(1)


class PlaceholderModule(QWidget):
    """Placeholder tab for planned functionality."""

    def __init__(self, title: str, description: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"<h3>{title}</h3>"))
        info = QLabel(description)
        info.setWordWrap(True)
        layout.addWidget(info)
        layout.addStretch(1)


class MainWindow(QMainWindow):
    """Main application window that hosts the start screen and module tabs."""

    def __init__(self, modules: List[ModuleInfo], ctx: ApplicationContext) -> None:
        super().__init__()
        self.setWindowTitle("Analiza połączeń lotniczych")
        self.setMinimumSize(1200, 720)
        self.setWindowIcon(QIcon())

        self._modules = modules
        self._ctx = ctx
        self._module_widgets: Dict[str, QWidget] = {}

        self._tabs = QTabWidget()
        self._tabs.setMovable(True)
        self._tabs.setTabsClosable(False)
        self.setCentralWidget(self._tabs)

        self._start_widget = StartScreenWidget(modules)
        self._start_widget.module_selected.connect(self._focus_module)

        self._tabs.addTab(self._start_widget, "Start")
        for module in modules:
            widget = self._create_module_widget(module)
            self._module_widgets[module.name] = widget
            self._tabs.addTab(widget, module.name)

    def _create_module_widget(self, module: ModuleInfo) -> QWidget:
        try:
            return module.factory(self._ctx)
        except Exception as exc:  # pragma: no cover - GUI feedback
            fallback = QWidget()
            layout = QVBoxLayout(fallback)
            label = QLabel(
                f"Błąd podczas ładowania modułu '{module.name}': {exc}"
            )
            label.setWordWrap(True)
            layout.addWidget(label)
            layout.addStretch(1)
            return fallback

    def _focus_module(self, module: ModuleInfo) -> None:
        widget = self._module_widgets.get(module.name)
        if widget is None:
            QMessageBox.warning(
                self,
                "Moduł niedostępny",
                f"Zakładka '{module.name}' nie jest dostępna.",
            )
            return
        index = self._tabs.indexOf(widget)
        if index != -1:
            self._tabs.setCurrentIndex(index)

    def closeEvent(self, event: QCloseEvent) -> None:  # pragma: no cover - GUI lifecycle
        super().closeEvent(event)


class ShortestRouteTab(QWidget):
    """Module responsible for the 'Najkrótsza trasa' functionality."""

    def __init__(self, ctx: ApplicationContext, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._ctx = ctx
        self._map_file: Optional[str] = None
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QHBoxLayout(self)

        # Left: map preview and summary
        left_container = QWidget()
        left_layout = QVBoxLayout(left_container)
        left_layout.setContentsMargins(0, 0, 10, 0)

        self.map_widget = (
            QWebEngineView() if QWebEngineView is not None else QLabel("Mapa wymaga modułu PySide6-QtWebEngine.")
        )
        if isinstance(self.map_widget, QLabel):
            self.map_widget.setAlignment(Qt.AlignCenter)
            self.map_widget.setWordWrap(True)
        elif QWebEngineSettings is not None:
            settings = self.map_widget.settings()
            settings.setAttribute(QWebEngineSettings.LocalContentCanAccessRemoteUrls, True)
            settings.setAttribute(QWebEngineSettings.LocalContentCanAccessFileUrls, True)
        left_layout.addWidget(self.map_widget, stretch=1)

        self.result_panel = QTextEdit()
        self.result_panel.setReadOnly(True)
        self.result_panel.setPlaceholderText("Po wygenerowaniu trasy pojawią się tutaj szczegóły lotu.")
        left_layout.addWidget(self.result_panel, stretch=0)

        layout.addWidget(left_container, stretch=2)

        # Right: form controls
        right_container = QWidget()
        right_layout = QVBoxLayout(right_container)
        right_layout.setAlignment(Qt.AlignTop)

        instructions = QLabel(
            "<b>Parametry wyszukiwania</b><br>"
            "Wybierz lotniska (kody ICAO) oraz tryb optymalizacji: najkrótszy dystans lub najszybszy lot."
        )
        instructions.setWordWrap(True)
        right_layout.addWidget(instructions)

        airports = self._ctx.data.airports

        self.source_input = self._create_airport_combobox(airports, placeholder="Lotnisko startowe (np. EPWA)")
        self.target_input = self._create_airport_combobox(airports, placeholder="Lotnisko docelowe (np. KLAX)")

        # Preselect common demo route if present
        self._set_airport_selection(self.source_input, "EPWA")
        self._set_airport_selection(self.target_input, "KLAX")

        right_layout.addWidget(QLabel("Lotnisko startowe:"))
        right_layout.addWidget(self.source_input)
        right_layout.addWidget(QLabel("Lotnisko docelowe:"))
        right_layout.addWidget(self.target_input)

        self.swap_button = QPushButton("Zamień lotniska")
        self.swap_button.clicked.connect(self._swap_airports)
        right_layout.addWidget(self.swap_button)

        self.mode_button = QPushButton()
        self.mode_button.setCheckable(True)
        self.mode_button.clicked.connect(self._toggle_mode)
        self._update_mode_button_text()
        right_layout.addWidget(self.mode_button)

        self.generate_button = QPushButton("Wygeneruj trasę")
        self.generate_button.clicked.connect(self._compute_route)
        right_layout.addWidget(self.generate_button)

        right_layout.addStretch(1)
        layout.addWidget(right_container, stretch=1)

    def _create_airport_combobox(self, airports: List[AirportRecord], placeholder: str) -> QComboBox:
        combo = QComboBox(self)
        combo.setEditable(True)
        combo.setInsertPolicy(QComboBox.NoInsert)
        combo.setPlaceholderText(placeholder)
        combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        for airport in airports:
            combo.addItem(airport.label, airport.code)
        # Use contains matching to search by code or name
        completer = combo.completer()
        if completer is not None:
            completer.setCaseSensitivity(Qt.CaseInsensitive)
            completer.setFilterMode(Qt.MatchContains)
        return combo

    def _set_airport_selection(self, combo: QComboBox, code: str) -> None:
        for idx in range(combo.count()):
            if combo.itemData(idx) == code:
                combo.setCurrentIndex(idx)
                return

    def _selected_code(self, combo: QComboBox) -> str:
        # Prefer stored item data; fall back to trimmed text if user typed
        data = combo.currentData()
        if isinstance(data, str) and data:
            return data.strip().upper()
        return (combo.currentText() or "").strip().upper()

    def _swap_airports(self) -> None:
        source_code = self._selected_code(self.source_input)
        target_code = self._selected_code(self.target_input)
        self._set_airport_selection(self.source_input, target_code)
        self._set_airport_selection(self.target_input, source_code)

    def _toggle_mode(self) -> None:
        self._update_mode_button_text()

    def _update_mode_button_text(self) -> None:
        if self.mode_button.isChecked():
            self.mode_button.setText("Tryb: Najszybszy lot")
        else:
            self.mode_button.setText("Tryb: Najkrótszy dystans")

    def _current_mode(self) -> str:
        return "time" if self.mode_button.isChecked() else "distance"

    def _compute_route(self) -> None:
        source_code = self._selected_code(self.source_input)
        target_code = self._selected_code(self.target_input)

        if not source_code or not target_code:
            QMessageBox.warning(self, "Brak danych", "Wybierz kody obu lotnisk z listy.")
            return
        if source_code == target_code:
            QMessageBox.warning(self, "Nieprawidłowe dane", "Lotniska muszą być różne.")
            return
        if not self._ctx.data.find_airport(source_code):
            QMessageBox.warning(self, "Nieznane lotnisko", f"Brak lotniska o kodzie {source_code}.")
            return
        if not self._ctx.data.find_airport(target_code):
            QMessageBox.warning(self, "Nieznane lotnisko", f"Brak lotniska o kodzie {target_code}.")
            return

        self.generate_button.setEnabled(False)
        self.result_panel.setPlainText("Trwa obliczanie trasy...")
        QApplication.processEvents()

        try:
            optimization = self._current_mode()
            if optimization == "time":
                route = shortest_path_time.fastest_route(source_code, target_code)
            else:
                route = shortest_path_distance.shortest_route(source_code, target_code)
        except Exception as exc:
            self.result_panel.setPlainText("")
            QMessageBox.critical(self, "Błąd zapytania", str(exc))
            self.generate_button.setEnabled(True)
            return

        self.generate_button.setEnabled(True)
        if route is None:
            self.result_panel.setPlainText("Brak trasy pomiędzy wskazanymi lotniskami.")
            self._clear_map()
            return

        self._display_route(route)

    def _display_route(self, route) -> None:
        stops = getattr(route, "stops", [])
        if not stops:
            self.result_panel.setPlainText("Nie udało się pobrać listy lotnisk dla trasy.")
            self._clear_map()
            return

        distance_nm = getattr(route, "total_distance_nm", None)
        time_min = getattr(route, "total_time_minutes", None)
        lines = [
            f"Lotnisko startowe: {stops[0].code}",
            f"Lotnisko docelowe: {stops[-1].code}",
        ]
        if distance_nm is not None:
            distance_km = distance_nm * 1.852
            lines.append(f"Długość trasy: {distance_nm:.2f} Nm ({distance_km:.2f} km)")
        if time_min is not None:
            lines.append(f"Czas podróży: {time_min:.2f} min ({time_min / 60.0:.2f} h)")
        lines.append("Lotniska pośrednie:")
        for stop in stops:
            desc_parts = [stop.code]
            if stop.name:
                desc_parts.append(stop.name)
            lines.append(f" • {' – '.join(desc_parts)}")

        self.result_panel.setText("\n".join(lines))
        self._render_map(stops)

    def _render_map(self, stops) -> None:
        if QWebEngineView is None:
            return

        coords = [
            (getattr(stop, "lat", None), getattr(stop, "lon", None), getattr(stop, "code", ""))
            for stop in stops
        ]
        if not all(lat is not None and lon is not None for lat, lon, _ in coords):
            self.result_panel.append("\nBrak pełnych współrzędnych dla wszystkich lotnisk – mapa niedostępna.")
            self._clear_map()
            return

        start_lat, start_lon, _ = coords[0]
        map_obj = folium.Map(location=[start_lat, start_lon], zoom_start=4, tiles="CartoDB positron")
        points = []
        for lat, lon, code in coords:
            folium.Marker(
                [lat, lon],
                tooltip=code,
                icon=folium.Icon(color="blue", icon="plane", prefix="fa"),
            ).add_to(map_obj)
            points.append([lat, lon])

        folium.PolyLine(points, color="red", weight=3, opacity=0.8).add_to(map_obj)

        if self._map_file:
            try:
                Path(self._map_file).unlink(missing_ok=True)
            except OSError:
                pass

        tmp_file = tempfile.NamedTemporaryFile(prefix="route_", suffix=".html", delete=False)
        map_obj.save(tmp_file.name)
        self._map_file = tmp_file.name
        tmp_file.close()

        self.map_widget.setUrl(QUrl.fromLocalFile(self._map_file))  # type: ignore[union-attr]

    def _clear_map(self) -> None:
        if QWebEngineView is None:
            return
        self.map_widget.setHtml("<p>Brak danych do wyświetlenia.</p>")  # type: ignore[union-attr]


class HubAnalysisTab(QWidget):
    """Module to detect hub airports using a Neo4j Cypher query."""

    def __init__(self, ctx: ApplicationContext, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._ctx = ctx
        self._map_file: Optional[str] = None
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QHBoxLayout()

        self.limit_input = QLineEdit("15")
        self.limit_input.setMaximumWidth(80)
        self.degree_input = QLineEdit("5000")
        self.degree_input.setMaximumWidth(100)

        form.addWidget(QLabel("Liczba wyników:"))
        form.addWidget(self.limit_input)
        form.addSpacing(12)
        form.addWidget(QLabel("Min. liczba relacji:"))
        form.addWidget(self.degree_input)
        form.addSpacing(12)

        self.run_button = QPushButton("Uruchom zapytanie")
        self.run_button.clicked.connect(self._run_query)
        form.addWidget(self.run_button)
        form.addStretch(1)

        layout.addLayout(form)

        self.status_label = QLabel("Gotowe")
        layout.addWidget(self.status_label)

        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels(
            [
                "Kod",
                "Lotnisko",
                "Kraj",
                "Operacje",
                "Kierunki",
                "Hub score",
                "Dominująca linia (%)",
            ]
        )
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table, stretch=1)

        self.map_widget = (
            QWebEngineView() if QWebEngineView is not None else QLabel("Mapa wymaga modułu PySide6-QtWebEngine.")
        )
        if isinstance(self.map_widget, QLabel):
            self.map_widget.setAlignment(Qt.AlignCenter)
            self.map_widget.setWordWrap(True)
        elif QWebEngineSettings is not None:
            settings = self.map_widget.settings()
            settings.setAttribute(QWebEngineSettings.LocalContentCanAccessRemoteUrls, True)
            settings.setAttribute(QWebEngineSettings.LocalContentCanAccessFileUrls, True)
        layout.addWidget(self.map_widget, stretch=1)
        self._clear_map()

    def _run_query(self) -> None:
        try:
            limit = int(self.limit_input.text())
            min_deg = int(self.degree_input.text())
        except ValueError:
            QMessageBox.warning(self, "Nieprawidłowe dane", "Limit i min. relacji muszą być liczbami całkowitymi.")
            return

        self.run_button.setEnabled(False)
        self.status_label.setText("Łączenie z Neo4j...")
        QApplication.processEvents()

        try:
            hubs = hub_analysis.fetch_hubs(limit=limit, min_degree=min_deg)
        except Exception as exc:
            QMessageBox.critical(self, "Błąd zapytania", str(exc))
            self.status_label.setText("Błąd: " + str(exc))
            self.run_button.setEnabled(True)
            return

        self._populate_table(hubs)
        self._render_map(hubs)
        self.status_label.setText(f"Pobrano {len(hubs)} wyników")
        self.run_button.setEnabled(True)

    def _populate_table(self, hubs: List[hub_analysis.HubAirport]) -> None:
        self.table.setRowCount(len(hubs))
        for row, hub in enumerate(hubs):
            self.table.setItem(row, 0, QTableWidgetItem(hub.code or ""))
            self.table.setItem(row, 1, QTableWidgetItem(hub.airport or ""))
            self.table.setItem(row, 2, QTableWidgetItem(hub.country or ""))
            self.table.setItem(row, 3, QTableWidgetItem(str(hub.total_ops)))
            self.table.setItem(row, 4, QTableWidgetItem(str(hub.unique_routes)))
            self.table.setItem(row, 5, QTableWidgetItem(f"{hub.hub_score:.0f}"))
            dom = hub.dominant_airline or ""
            share = f" ({hub.airline_share_pct:.1f}%)" if hub.airline_share_pct is not None else ""
            self.table.setItem(row, 6, QTableWidgetItem(dom + share))

    def _render_map(self, hubs: List[hub_analysis.HubAirport]) -> None:
        if QWebEngineView is None:
            return

        points = []
        for hub in hubs:
            airport = self._ctx.data.find_airport(hub.code)
            if airport and airport.lat is not None and airport.lon is not None:
                label = f"{hub.code} — {hub.airport or airport.name}"
                points.append((airport.lat, airport.lon, label, hub.hub_score))

        if not points:
            self._clear_map()
            return

        center_lat, center_lon, *_ = points[0]
        map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=4, tiles="CartoDB positron")

        max_score = max(p[3] for p in points) or 1.0
        for lat, lon, label, score in points:
            radius = max(5, min(18, (score / max_score) * 18))
            folium.CircleMarker(
                location=[lat, lon],
                radius=radius,
                popup=label,
                tooltip=label,
                color="#1f77b4",
                fill=True,
                fill_color="#1f77b4",
                fill_opacity=0.75,
            ).add_to(map_obj)

        if self._map_file:
            try:
                Path(self._map_file).unlink(missing_ok=True)
            except OSError:
                pass

        tmp_file = tempfile.NamedTemporaryFile(prefix="hubs_", suffix=".html", delete=False)
        map_obj.save(tmp_file.name)
        self._map_file = tmp_file.name
        tmp_file.close()

        self.map_widget.setUrl(QUrl.fromLocalFile(self._map_file))  # type: ignore[union-attr]

    def _clear_map(self) -> None:
        if QWebEngineView is None:
            return
        self.map_widget.setHtml("<p>Brak danych do wyświetlenia.</p>")  # type: ignore[union-attr]


def build_modules(ctx: ApplicationContext) -> List[ModuleInfo]:
    """Register modules that are available in the GUI."""

    modules: List[ModuleInfo] = [
        ModuleInfo(
            name="Najkrótsza trasa",
            description="Znajdź optymalną trasę między lotniskami z uwzględnieniem dystansu lub czasu podróży.",
            factory=lambda ctx: ShortestRouteTab(ctx),
        ),
        ModuleInfo(
            name="Analiza hubów",
            description="Wykrywanie najważniejszych lotnisk-hubów na podstawie zapytania Cypher.",
            factory=lambda ctx: HubAnalysisTab(ctx),
        ),
        ModuleInfo(
            name="Statystyki popularności",
            description="Planowana zakładka do eksploracji statystyk przewozów i natężenia ruchu.",
            factory=lambda ctx: PlaceholderModule(
                "Statystyki popularności",
                "Moduł w przygotowaniu. Pozwoli porównywać ruch pasażerski i liczbę połączeń w czasie.",
            ),
        ),
    ]
    return modules


def create_context() -> ApplicationContext:
    repo_root = Path(__file__).resolve().parent
    data_repo = DataRepository(repo_root / "airports_mapping.csv")
    return ApplicationContext(data_repo)


def main() -> None:  # pragma: no cover - GUI bootstrap
    app = QApplication(sys.argv)
    ctx = create_context()
    modules = build_modules(ctx)
    window = MainWindow(modules, ctx)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":  # pragma: no cover
    main()
