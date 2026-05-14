#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# 🧭 START-HINWEISE – Server / Main / Entry-Point
# =============================================================================
# Das Projekt verwendet einen dynamischen Entry-Point (src/entry.py),
# der automatisch alle *_main.py Dateien erkennt und ausführen kann.
#
# ---------------------------------------------------------------------------
# 📦 Standard-Layout (Beispiel)
# ---------------------------------------------------------------------------
# project_root/
# ├─ src/
# │   ├─ entry.py                ← dynamischer Starter
# │   ├─ async_xserver_main.py   ← Server-Main
# │   ├─ data_probe_main.py      ← anderes Main-Modul
# │   └─ ...
#
# ---------------------------------------------------------------------------
# 🚀 STARTVARIANTEN
# ---------------------------------------------------------------------------
#
# 1️⃣  Direkter Aufruf aus dem Entwicklungs-Repository:
#     (setzt voraus, dass du dich im Projekt-Root befindest)
#
#     # Server starten:
#     python src/entry.py async_xserver
#
#     # Alternatives Main-Modul starten:
#     python src/entry.py data_probe
#
#     # Alle verfügbaren Mains anzeigen:
#     python src/entry.py
#
# ---------------------------------------------------------------------------
# 2️⃣  Sauberer Modulstart über Python-Package-System:
#     (src ist als Package, daher absoluter Import-Pfad gültig)
#
#     python -m src.entry async_xserver
#
# ---------------------------------------------------------------------------
# 3️⃣  Nach Installation über setup.cfg / pyproject.toml:
#     (Entry-Point ist als "run = src.entry:main" definiert)
#
#     # Listet verfügbare Main-Module
#     run
#
#     # Startet den Server
#     run async_xserver
#
#     # Startet alternative Komponente
#     run data_probe
#
# ---------------------------------------------------------------------------
# 4️⃣  Alternative manuelle Ausführung einzelner Main-Dateien:
#     (nur im Entwicklungsmodus empfohlen)
#
#     # Direkt ausführen
#     python -m src.async_xserver_main
#
#     # Oder einfach
#     python src/async_xserver_main.py
#
#     Hinweis:
#         Bei dieser Variante müssen alle Importe auf "src." basieren.
#         Empfohlen ist immer die -m / entry.py Methode.
#
# ---------------------------------------------------------------------------
# 5️⃣  Optional mit Logging / Debugging:
#
#     python -m src.entry async_xserver --log-level DEBUG
#     python src/entry.py data_probe --verbose
#
# =============================================================================


"""
Universeller Starter für das Event-Driven-System.

Dieses Skript befindet sich im Projekt-Root und ruft automatisch
den dynamischen Entry-Point (src/entry.py) mit korrektem sys.path auf.

Beispiele:
    python run.py async_xserver
    python run.py data_probe
    python run.py               # listet verfügbare Main-Module


# zeigt verfügbare Mains
python run.py

# startet async_xserver_main.py
python run.py async_xserver

# startet ein anderes Main
python run.py data_probe

# übergibt Argumente an das Ziel-Main
python run.py async_xserver --debug --threads 4

"""

from __future__ import annotations
import sys
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).parent.resolve()

python_exec = sys.executable
args = sys.argv[1:]

# Starte im Modulmodus – wichtig: cwd=ROOT, damit 'src' als Paket gefunden wird
cmd = [python_exec, "-m", "src.entry"] + args

print(f"[INFO] Starte: {' '.join(cmd)}\n")
try:
    result = subprocess.run(cmd, check=False, cwd=str(ROOT))
    sys.exit(result.returncode)
except KeyboardInterrupt:
    print("\n[INFO] Abbruch durch Benutzer.")
    sys.exit(130)
except Exception as e:
    print(f"[ERROR] Fehler beim Start: {e}")
    sys.exit(2)