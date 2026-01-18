"""Desktop GUI for analyzing flight connections with modular tabs."""

from __future__ import annotations

import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional
import os
import uuid
from typing import List, Dict
import matplotlib.pyplot as plt

import folium
import pandas as pd
from PySide6.QtCore import Qt, QUrl, Signal
from PySide6.QtGui import QCloseEvent, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QSplitter,
    QComboBox,
)

try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWebEngineCore import QWebEngineSettings
except Exception:  # pragma: no cover - optional dependency for lightweight runs
    QWebEngineView = None  # type: ignore[assignment]
    QWebEngineSettings = None  # type: ignore[assignment]

import shortest_path_distance
import shortest_path_time


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
        else:
            settings = self.map_widget.settings()
            if QWebEngineSettings is not None:
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



class PopularityStatsTab(QWidget):

    def __init__(self, ctx, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._ctx = ctx

        # Słownik tłumaczeń (English -> Polish)
        self.pl_names = {
            "United Kingdom": "Wielka Brytania", "United States": "USA", "Germany": "Niemcy",
            "France": "Francja", "Spain": "Hiszpania", "Italy": "Włochy", "Poland": "Polska",
            "Ireland": "Irlandia", "Netherlands": "Holandia", "Belgium": "Belgia",
            "Switzerland": "Szwajcaria", "Austria": "Austria", "Portugal": "Portugalia",
            "Greece": "Grecja", "Sweden": "Szwecja", "Norway": "Norwegia", "Denmark": "Dania",
            "Finland": "Finlandia", "Russia": "Rosja", "Ukraine": "Ukraina",
            "Czech Republic": "Czechy", "Hungary": "Węgry", "Turkey": "Turcja",
            "Romania": "Rumunia", "Bulgaria": "Bułgaria", "Croatia": "Chorwacja",
            "Slovakia": "Słowacja", "Lithuania": "Litwa", "Latvia": "Łotwa",
            "Estonia": "Estonia", "Belarus": "Białoruś", "China": "Chiny", "Japan": "Japonia",
            "Canada": "Kanada", "Australia": "Australia", "India": "Indie",
            "United Arab Emirates": "ZEA", "Egypt": "Egipt", "Israel": "Izrael",
            "Cyprus": "Cypr", "Malta": "Malta", "Iceland": "Islandia"
        }

        # Konfiguracja plików
        self.annual_sources = {
            "2017": "reports/report_country_connections_2017.csv",
            "2018": "reports/report_country_connections_2018.csv"
        }
        self.monthly_sources = {
            "2017": "reports/monthly_flight_report_2017.csv",
            "2018": "reports/monthly_flight_report_2018.csv"
        }
        self.population_file = "population.csv"

        self.current_annual_df: pd.DataFrame = pd.DataFrame()
        self.current_monthly_df: pd.DataFrame = pd.DataFrame()
        self.population_df: pd.DataFrame = pd.DataFrame()

        self._load_population_data()
        self._build_ui()
        self._load_data_for_year("2017")

    def _translate(self, name: str) -> str:
        """Tłumaczy nazwę kraju na polski."""
        return self.pl_names.get(name, name)

    def _load_population_data(self):
        if not os.path.exists(self.population_file): return
        try:
            df = pd.read_csv(self.population_file)
            if 'Geopolitical entity (reporting)' in df.columns:
                df = df[['Geopolitical entity (reporting)', 'TIME_PERIOD', 'OBS_VALUE']]
                df.columns = ['Country', 'Year', 'Population']
                df['Population'] = pd.to_numeric(df['Population'], errors='coerce')
                df.dropna(subset=['Population'], inplace=True)
                self.population_df = df
        except:
            pass

    def _build_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # HEADER
        controls_frame = QFrame()
        controls_frame.setStyleSheet("background-color: #f0f0f0; border-bottom: 1px solid #ccc;")
        controls_frame.setFixedHeight(60)
        controls_layout = QHBoxLayout(controls_frame)

        controls_layout.addWidget(QLabel("<b>Rok:</b>"))
        self.year_combo = QComboBox()
        self.year_combo.addItems(list(self.annual_sources.keys()))
        self.year_combo.setFixedWidth(80)
        self.year_combo.currentTextChanged.connect(self._on_year_changed)
        controls_layout.addWidget(self.year_combo)

        controls_layout.addWidget(QLabel("<b>Kraj:</b>"))
        self.country_combo = QComboBox()
        self.country_combo.setEditable(True)
        self.country_combo.setMinimumWidth(200)
        self.country_combo.currentTextChanged.connect(self._on_country_changed)
        controls_layout.addWidget(self.country_combo)

        controls_layout.addStretch(1)
        main_layout.addWidget(controls_frame, 0)

        # SPLITTER
        splitter = QSplitter(Qt.Horizontal)
        self.global_panel = QTextEdit()
        self.global_panel.setReadOnly(True)
        self.country_panel = QTextEdit()
        self.country_panel.setReadOnly(True)

        splitter.addWidget(self._create_panel("GLOBALNE", self.global_panel))
        splitter.addWidget(self._create_panel("SZCZEGÓŁY KRAJU", self.country_panel))
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 2)
        main_layout.addWidget(splitter, 1)

    def _create_panel(self, title: str, w: QWidget) -> QWidget:
        c = QWidget()
        l = QVBoxLayout(c)
        l.setContentsMargins(0, 0, 0, 0)
        l.setSpacing(0)
        h = QLabel(title)
        h.setAlignment(Qt.AlignCenter)
        h.setStyleSheet("background:#e0e0e0; font-weight:bold; padding:6px; border-bottom:1px solid #ccc;")
        l.addWidget(h)
        l.addWidget(w)
        return c

    def _load_data_for_year(self, year: str) -> None:
        # 1. Wczytaj dane roczne
        try:
            self.current_annual_df = pd.read_csv(self.annual_sources.get(year, ""))
        except:
            self.current_annual_df = pd.DataFrame()

        # 2. Wczytaj dane miesięczne i FILTRUJ MIESIĄCE
        try:
            self.current_monthly_df = pd.read_csv(self.monthly_sources.get(year, ""))
            if 'month' in self.current_monthly_df.columns:
                # Formatowanie miesiąca do "01", "02" etc.
                self.current_monthly_df['month'] = self.current_monthly_df['month'].astype(str).str.zfill(2)

                # --- FILTRACJA: TYLKO Marzec (03), Czerwiec (06), Wrzesień (09), Grudzień (12) ---
                allowed_months = ['03', '06', '09', '12']
                self.current_monthly_df = self.current_monthly_df[
                    self.current_monthly_df['month'].isin(allowed_months)
                ]
        except:
            self.current_monthly_df = pd.DataFrame()

        # Odśwież UI
        self._populate_countries()
        self._update_global_stats()

        # Pobierz aktualnie wybrany kraj (jego angielską nazwę "pod spodem")
        idx = self.country_combo.currentIndex()
        if idx >= 0:
            english_name = self.country_combo.itemData(idx)
            self._update_country_stats(english_name)

    def _populate_countries(self) -> None:
        if self.current_annual_df.empty: return

        # Pobierz unikalne kraje (nazwy angielskie z CSV)
        countries_en = set(self.current_annual_df['origin_country'].unique()) | \
                       set(self.current_annual_df['destination_country'].unique())

        # Sortuj według polskich nazw
        sorted_list = sorted(list(countries_en), key=lambda x: self._translate(x))

        self.country_combo.blockSignals(True)
        self.country_combo.clear()
        # Dodajemy: Tekst = Polska nazwa, Data = Angielska nazwa
        for country in sorted_list:
            self.country_combo.addItem(self._translate(country), country)

        self.country_combo.blockSignals(False)
        if self.country_combo.count() > 0: self.country_combo.setCurrentIndex(0)

    def _on_year_changed(self, y):
        self._load_data_for_year(y)

    def _on_country_changed(self, index):
        # Pobieramy ukrytą angielską nazwę do filtrowania danych
        english_name = self.country_combo.itemData(self.country_combo.currentIndex())
        if english_name:
            self._update_country_stats(english_name)

    def _update_global_stats(self):
        if self.current_annual_df.empty: return
        df = self.current_annual_df

        # 1. TOP 5 DESTYNACJI
        top_dest = df.groupby('destination_country')['flights'].sum().sort_values(ascending=False).head(5)

        html = f"<h3>Top 5 Destynacji</h3><ol>"
        for c, v in top_dest.items(): html += f"<li>{self._translate(c)}: <b>{v}</b></li>"
        html += "</ol>"

        # 2. TOP 5 TRAS (Przywrócone)
        inter_df = df[df['origin_country'] != df['destination_country']]
        top_routes = inter_df.sort_values(by='flights', ascending=False).head(5)

        html += "<hr><h3>Top 5 Tras</h3><ul>"
        for _, row in top_routes.iterrows():
            orig = self._translate(row['origin_country'])
            dest = self._translate(row['destination_country'])
            html += f"<li>{orig} &rarr; {dest} ({row['flights']})</li>"
        html += "</ul>"

        # 3. NORMALIZACJA (Loty / Osobę)
        if not self.population_df.empty:
            try:
                inc = df.groupby('destination_country')['flights'].sum().reset_index()
                inc.columns = ['Country', 'Incoming_Flights']
                pop = self.population_df[self.population_df['Year'] == int(self.year_combo.currentText())]
                m = pd.merge(inc, pop, on='Country')
                m['PC'] = m['Incoming_Flights'] / m['Population']
                top = m.sort_values('PC', ascending=False).head(7)

                html += "<hr><h3>Loty / Osobę (Top 7)</h3><table width='100%' cellpadding='2'>"
                for i, r in enumerate(top.itertuples(), 1):
                    pl_country = self._translate(r.Country)
                    html += f"<tr><td>{i}. {pl_country}</td><td align='right'><b>{r.PC:.4f}</b></td></tr>"
                html += "</table>"
            except:
                pass
        self.global_panel.setHtml(html)

    def _generate_seasonality_chart(self, country: str) -> Optional[str]:
        if self.current_monthly_df.empty: return None
        # Filtrujemy po angielskiej nazwie
        data = self.current_monthly_df[self.current_monthly_df['origin_country'] == country]
        if data.empty: return None

        # Grupuj po miesiącach (powinny zostać tylko 03, 06, 09, 12)
        grp = data.groupby('month')['flights'].sum().reset_index().sort_values('month')

        # Mały rozmiar wykresu
        plt.figure(figsize=(4.5, 2.2))

        plt.plot(grp['month'], grp['flights'], marker='o', color='#2980b9', linewidth=2, markersize=5)
        # Opcjonalne: jeśli chcesz tylko punkty bez linii (bo są dziury w miesiącach), usuń linestyle
        # plt.plot(grp['month'], grp['flights'], 'o', color='#2980b9')

        pl_name = self._translate(country)
        plt.title(f"Sezonowość (kwartały): {pl_name}", fontsize=9)
        plt.grid(True, alpha=0.3)
        plt.xticks(fontsize=7)
        plt.yticks(fontsize=7)
        plt.xlabel("Miesiąc", fontsize=7)

        plt.tight_layout(pad=0.4)

        fname = f"temp/chart_{uuid.uuid4().hex[:6]}.png"
        path = os.path.abspath(fname)
        plt.savefig(path, dpi=100)
        plt.close()
        return path.replace('\\', '/')

    def _update_country_stats(self, country: str):
        if self.current_annual_df.empty or not country: return
        df = self.current_annual_df
        pl_country = self._translate(country)

        out = df[df['origin_country'] == country]
        inc = df[df['destination_country'] == country]

        # 1. TYTUŁ
        html = f"<h2 style='margin:0; text-align:center; color:#2c3e50;'>{pl_country}</h2>"

        # 2. WYKRES SEZONOWOŚCI (Na górze)
        chart = self._generate_seasonality_chart(country)
        if chart:
            html += f"<div style='text-align:center; margin-top:5px;'><img src='{chart}' width='400'></div>"
        else:
            html += "<p style='color:gray; text-align:center; font-size:10px;'>Brak danych dla wybranych miesięcy.</p>"

        # 3. STATYSTYKI OGÓLNE
        tot_out = out['flights'].sum()
        tot_in = inc['flights'].sum()
        html += f"""
        <table width='100%' cellpadding='4' cellspacing='0' style='background-color:#f8f9fa; margin-top:5px; border:1px solid #ddd; font-size:11px;'>
            <tr><td align='center' style='color:green'><b>Wyloty:</b> {tot_out}</td>
                <td align='center' style='color:orange'><b>Przyloty:</b> {tot_in}</td></tr>
        </table>
        """

        # 4. TABELA WYLOTÓW (Top 7) - Tłumaczona
        if not out.empty:
            top = out.sort_values('flights', ascending=False).head(7)
            html += "<h4 style='margin-bottom:2px; margin-top:10px; border-bottom:1px solid #eee;'>Top 7 Wylotów</h4>"
            html += "<table width='100%' cellspacing='0' cellpadding='2' style='font-size:11px;'>"
            for i, (_, r) in enumerate(top.iterrows()):
                bg = "#f9f9f9" if i % 2 == 0 else "#fff"
                dest_pl = self._translate(r['destination_country'])
                html += f"<tr style='background:{bg}'><td>{dest_pl}</td><td align='right'><b>{r['flights']}</b></td></tr>"
            html += "</table>"

        # 5. TABELA PRZYLOTÓW (Top 7) - Tłumaczona
        if not inc.empty:
            top = inc.sort_values('flights', ascending=False).head(7)
            html += "<h4 style='margin-bottom:2px; margin-top:10px; border-bottom:1px solid #eee;'>Top 7 Przylotów</h4>"
            html += "<table width='100%' cellspacing='0' cellpadding='2' style='font-size:11px;'>"
            for i, (_, r) in enumerate(top.iterrows()):
                bg = "#f9f9f9" if i % 2 == 0 else "#fff"
                orig_pl = self._translate(r['origin_country'])
                html += f"<tr style='background:{bg}'><td>{orig_pl}</td><td align='right'><b>{r['flights']}</b></td></tr>"
            html += "</table>"

        self.country_panel.setHtml(html)

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
            description="Przyszła rozbudowa: wykrywanie najważniejszych lotnisk-hubów przy użyciu centralności.",
            factory=lambda ctx: PlaceholderModule(
                "Analiza hubów",
                "Moduł w przygotowaniu. W tym miejscu pojawi się analiza stopnia centralności "
                "oraz ranking lotnisk pełniących rolę węzłów przesiadkowych.",
            ),
        ),
        ModuleInfo(
            name="Statystyki popularności",
            description="Planowana zakładka do eksploracji statystyk przewozów i natężenia ruchu.",
            factory=lambda ctx:PopularityStatsTab(ctx),
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
