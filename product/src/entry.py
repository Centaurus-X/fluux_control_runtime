#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dynamischer Entry-Point für alle *_main.py Module im src/ Verzeichnis.

src/entry.py

Aufruf:
    run async_xserver
    run data_probe
    run network_agent
"""
from __future__ import annotations
import sys
import importlib
import pathlib
import inspect

# --- Einheitlicher Bootstrap --------------------------------------------------
try:
    from src import bootstrap as _bootstrap
except ModuleNotFoundError:
    import bootstrap as _bootstrap  # falls aus project_root/src per -m gestartet
_bootstrap.apply()

def discover_mains(base: pathlib.Path):
    """Finde alle *_main.py Dateien rekursiv unterhalb von base."""
    for f in base.rglob("*_main.py"):
        yield f.stem.replace("_main", ""), f


def _call_main_adaptively(mod, args):
    """
    Ruft mod.main adaptiv auf:
      - ohne Args, wenn die Signatur keine Parameter vorsieht
      - mit args (Liste), wenn mind. 1 Parameter vorhanden ist
    Rückgabe: int Exit-Code oder 0/1 Default.
    """
    if not hasattr(mod, "main"):
        print(f"[ERROR] Modul '{mod.__name__}' besitzt keine main()-Funktion.")
        return 3

    fn = mod.main
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        if not params:
            rv = fn()
        else:
            rv = fn(args)
    except TypeError:
        # Fallback: falls Signatur ungewöhnlich ist
        try:
            rv = fn()
        except Exception as e:
            print(f"[ERROR] Aufruf von main() scheiterte: {e}")
            return 4
    except Exception as e:
        print(f"[ERROR] Aufruf von main() scheiterte: {e}")
        return 4

    # Exit-Code normieren
    if isinstance(rv, int):
        return rv
    return 0 if rv is None else 0


# --- NEU: tolerante Zielauflösung -------------------------------------------

def _normalize_name(text: str) -> str:
    """
    Normalisiert Zielnamen für robuste Vergleiche:
    - Kleinbuchstaben
    - Nur [a-z0-9]
    Beispiel: 'Async_XServer-Clear' -> 'asyncxserverclear'
    """
    lowered = text.lower()
    return "".join(ch for ch in lowered if ch.isalnum())


def _resolve_target_and_extras(
    raw_target: str,
    candidates: dict[str, "pathlib.Path"]
) -> tuple[str | None, list[str]]:
    """
    Versucht, den vom Nutzer übergebenen Zielnamen auf einen bekannten Kandidaten
    abzubilden. Akzeptiert:
      - exakte Namen (z. B. 'async_xserver')
      - normalisierte Übereinstimmungen ('asyncxserver')
      - 'verklebte' Varianten mit Suffix (z. B. 'async_xserverclear' -> Ziel
        'async_xserver' + Extra-Arg ['clear']).
    Liefert (resolved_name, extra_args) oder (None, []).
    """
    norm_target = _normalize_name(raw_target)

    # 1) Exakter Treffer
    if raw_target in candidates:
        return raw_target, []

    # 2) Fall-/Separator-unabhängiger Treffer
    for name in candidates:
        if norm_target == _normalize_name(name):
            return name, []

    # 3) Längstes Kandidaten-Präfix der normalisierten Eingabe
    best_name = None
    best_len = -1
    for name in candidates:
        nn = _normalize_name(name)
        if norm_target.startswith(nn) and len(nn) > best_len:
            best_name = name
            best_len = len(nn)

    if best_name is not None:
        remainder = norm_target[best_len:]
        extra = [remainder] if remainder else []
        return best_name, extra

    # 4) Fuzzy-Fallback (sanfter Vorschlag)
    try:
        import difflib as _difflib  # lokal importiert, um Startzeit minimal zu halten
        matches = _difflib.get_close_matches(raw_target, list(candidates.keys()), n=1, cutoff=0.6)
        if matches:
            return matches[0], []
    except Exception:
        pass

    return None, []


# --- ERSETZT die vorhandene main() komplett ---------------------------------

def main(argv=None):
    import sys
    import pathlib
    import importlib

    argv = argv or sys.argv[1:]
    base = pathlib.Path(__file__).parent

    # Keine Argumente → Liste anzeigen
    if not argv:
        print("Verfügbare Startmodule:\n")
        for name, path in sorted(discover_mains(base)):
            rel = path.relative_to(base)
            print(f"  {name:<20} → {rel}")
        print("\nVerwende: run <name>")
        sys.exit(0)

    raw_target = argv[0]
    candidates = dict(discover_mains(base))

    resolved_name, extras = _resolve_target_and_extras(raw_target, candidates)
    if resolved_name is None:
        print(f"[ERROR] Kein Modul namens '{raw_target}' gefunden.")
        print("Verfügbare Optionen:", ", ".join(sorted(candidates.keys())))
        sys.exit(2)

    rel_path = candidates[resolved_name].relative_to(base)
    module_name = f"src.{'.'.join(rel_path.with_suffix('').parts)}"

    try:
        mod = importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        print(f"[ERROR] Modul '{module_name}' konnte nicht importiert werden: {e}")
        sys.exit(4)

    # verbleibende Args + aus dem 'verklebt' abgeleitete Extras
    args_for_main = argv[1:] + extras

    code = _call_main_adaptively(mod, args_for_main)
    sys.exit(code)


if __name__ == "__main__":
    try:
        import src  # Prüfen, ob Paket verfügbar
    except ModuleNotFoundError:
        import sys as _sys
        import pathlib as _pathlib
        # WICHTIG: Projekt-Root (Eltern von 'src') auf sys.path
        _sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parents[1]))
    main()
