"""Simple Tkinter app to show the shortest airport route by distance or time."""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
import tempfile
from pathlib import Path

from tkinterweb import HtmlFrame

import folium

from smoska.shortest_path_distance import ShortestRoute, shortest_route
from smoska.shortest_path_time import FastestRoute, fastest_route


def _format_distance(result: ShortestRoute) -> str:
    parts = [f"Total distance: {result.total_distance_nm:.1f} nm"]
    for idx, stop in enumerate(result.stops, start=1):
        parts.append(
            f"{idx}. {stop.code} - {stop.name or 'Unknown'}"
            f" (lat: {stop.lat if stop.lat is not None else 'n/a'},"
            f" lon: {stop.lon if stop.lon is not None else 'n/a'})"
        )
    return "\n".join(parts)


def _format_time(result: FastestRoute) -> str:
    parts = [f"Total time: {result.total_time_minutes:.1f} min ({result.total_time_minutes/60:.2f} h)"]
    for idx, stop in enumerate(result.stops, start=1):
        parts.append(
            f"{idx}. {stop.code} - {stop.name or 'Unknown'}"
            f" (lat: {stop.lat if stop.lat is not None else 'n/a'},"
            f" lon: {stop.lon if stop.lon is not None else 'n/a'})"
        )
    return "\n".join(parts)


def _render_map(stops) -> Path | None:
    # Only render if we have coordinates for at least two stops
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


_map_window: tk.Toplevel | None = None
_map_frame: HtmlFrame | None = None


def _open_map(stops, root: tk.Tk | tk.Toplevel | None = None) -> None:
    html_path = _render_map(stops)
    if html_path is None:
        messagebox.showinfo("Brak danych", "Brak współrzędnych aby narysować trasę")
        return

    global _map_window, _map_frame
    if _map_window is None or not _map_window.winfo_exists():
        parent = root if root is not None else tk.Tk()
        _map_window = tk.Toplevel(parent)
        _map_window.title("Mapa trasy")
        _map_window.geometry("900x700")
        _map_window.rowconfigure(0, weight=1)
        _map_window.columnconfigure(0, weight=1)
        _map_frame = HtmlFrame(_map_window, horizontal_scrollbar="auto")
        _map_frame.grid(row=0, column=0, sticky="nsew")
    if _map_frame is not None:
        _map_frame.load_file(str(html_path))


# Use a global variable to track the last result's stops for the "Pokaż mapę" button
_last_stops: list | None = None


def _on_submit(src_var: tk.StringVar, dst_var: tk.StringVar, mode_var: tk.StringVar, output: tk.Text, root: tk.Tk) -> None:
    source = src_var.get().strip()
    target = dst_var.get().strip()
    output.configure(state="normal")
    output.delete("1.0", tk.END)
    output.configure(state="disabled")

    if not source or not target:
        messagebox.showwarning("Missing data", "Wpisz oba kody lotnisk")
        return

    try:
        if mode_var.get() == "time":
            result = fastest_route(source, target)
            formatter = _format_time
        else:
            result = shortest_route(source, target)
            formatter = _format_distance
    except ValueError as exc:
        messagebox.showerror("Błąd", str(exc))
        return
    except Exception as exc:  # Neo4j connectivity etc.
        messagebox.showerror("Błąd", f"Nie udało się pobrać trasy: {exc}")
        return

    if result is None:
        messagebox.showinfo("Brak trasy", "Nie znaleziono połączenia między lotniskami")
        return

    output.configure(state="normal")
    output.insert(tk.END, formatter(result))
    output.configure(state="disabled")
    global _last_stops
    _last_stops = result.stops
    _open_map(result.stops, root)


def main() -> None:
    root = tk.Tk()
    root.title("Najkrótsza trasa lotnicza")

    src_var = tk.StringVar()
    dst_var = tk.StringVar()
    mode_var = tk.StringVar(value="distance")

    frame = ttk.Frame(root, padding=12)
    frame.grid(row=0, column=0, sticky="nsew")

    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    frame.columnconfigure(1, weight=1)

    ttk.Label(frame, text="Kod startowy:").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
    ttk.Entry(frame, textvariable=src_var).grid(row=0, column=1, sticky="ew", pady=4)

    ttk.Label(frame, text="Kod docelowy:").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
    ttk.Entry(frame, textvariable=dst_var).grid(row=1, column=1, sticky="ew", pady=4)

    mode_frame = ttk.LabelFrame(frame, text="Tryb trasy")
    mode_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 4))
    ttk.Radiobutton(mode_frame, text="Dystans", value="distance", variable=mode_var).grid(row=0, column=0, padx=6, pady=4)
    ttk.Radiobutton(mode_frame, text="Czas", value="time", variable=mode_var).grid(row=0, column=1, padx=6, pady=4)

    output = tk.Text(frame, height=10, width=50, state="disabled")
    output.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
    frame.rowconfigure(3, weight=1)

    btn_frame = ttk.Frame(frame)
    btn_frame.grid(row=4, column=0, columnspan=2, pady=6)

    ttk.Button(
        btn_frame,
        text="Pokaż trasę",
        command=lambda: _on_submit(src_var, dst_var, mode_var, output, root),
    ).grid(row=0, column=0, padx=4)
    ttk.Button(
        btn_frame,
        text="Pokaż mapę",
        command=lambda: _open_map(_last_stops or [], root),
    ).grid(row=0, column=1, padx=4)

    root.mainloop()


if __name__ == "__main__":
    main()
