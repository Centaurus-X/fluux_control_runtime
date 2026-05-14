# -*- coding: utf-8 -*-

# src/libraries/_json_formator.py

"""
JSON-Formatierer (kompatibel, keine Strukturänderungen)

Ziele:
- Arrays aus Objekten jeweils: ein Objekt pro Zeile (Einzeiler pro Element)
- Top-Level-Abschnitte bekommen gezielte Leerzeilen wie in ___device_state_data_.json
- Spezialfall dynamic_rule_engine.rule_sets:
    -> Der einzelne Rule-Set wird *nicht* als kompletter Einzeiler ausgegeben,
       damit dessen Feld "rules" als Liste mit "eine Regel pro Zeile" erscheint.
- Spezialfall automation_process_state_mappings:
    -> Die Mapping-Objekte werden rekursiv (mehrzeilig) ausgegeben, damit die
       Unterlisten (sensor_filters / automation_rules) wieder sauber als
       "ein Element pro Zeile" erscheinen.
- Optional: atomar in-place schreiben (sicheres Überschreiben der Eingabedatei)
- Neu: Sortierung direkt vor der Formatierung:
    -> Standardmäßig numerisch nach Primär-ID (erste numerische ID im Datensatz)
    -> Optional weiterhin referenzbasiert, alphabetisch oder nach Tabellen-Größe

Richtlinien:
- Keine Decorators (@)
- Keine dataclass
- functools.partial statt lambda
- Keine Strukturänderungen durch die Formatierung / Sortierung
"""

import argparse
import json
import os
import re
import sys
import tempfile
from functools import partial
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union


JSONType = Union[Dict[str, Any], List[Any], str, int, float, bool, None]

DEFAULT_REFERENCE_CANDIDATES = [
    "___device_state_data.json",
    "___device_state_data_.json",
    "device_state_data.json",
    "device_state_data_.json",
]

DEFAULT_TOP_SPACING = {
    "process_states": 2,
    "synthax_standard": 2,
    "automation": 1,
    "sensor_filters": 1,
    "process_state_mappings": 1,
    "control_method_templates": 1,
    "control_methods": 1,
    "process_timers": 1,
    "triggers": 1,
    "actions": 1,
    "transitions": 1,
    "dynamic_rule_engine": 1,
}

DEFAULT_SORT_MODE = "numeric_pk"

SORT_MODE_ALIASES = {
    "numeric_pk": "numeric_pk",
    "numeric": "numeric_pk",
    "id": "numeric_pk",
    "ids": "numeric_pk",
    "pk": "numeric_pk",
    "primary_key": "numeric_pk",
    "default": "numeric_pk",
    "reference": "reference",
    "ref": "reference",
    "alpha": "alpha",
    "alphabetic": "alpha",
    "alphabetisch": "alpha",
    "table_size": "table_size",
    "size": "table_size",
    "none": "none",
    "off": "none",
    "keep": "none",
    "preserve": "none",
}

INTEGER_STRING_PATTERN = re.compile(r"^[+-]?\d+$")


def _dump_inline_factory():
    return partial(json.dumps, ensure_ascii=False, separators=(",", ": "))


def _json_dumps_compact(wert: Any) -> str:
    return json.dumps(wert, ensure_ascii=False, separators=(",", ":"))


def _ist_dynamic_rule_sets_pfad(pfad: Sequence[str]) -> bool:
    if not isinstance(pfad, Sequence):
        return False
    if len(pfad) < 2:
        return False
    return pfad[-1] == "rule_sets" and pfad[-2] == "dynamic_rule_engine"


def _ist_automation_process_state_mappings_pfad(pfad: Sequence[str]) -> bool:
    if not isinstance(pfad, Sequence):
        return False
    if len(pfad) < 1:
        return False
    return pfad[-1] == "automation_process_state_mappings"


def _ist_id_schluessel(schluessel: Any) -> bool:
    if not isinstance(schluessel, str):
        return False

    text = schluessel.strip().lower()
    if not text:
        return False

    return text == "id" or text.endswith("_id") or text.endswith("_ids")


def _konvertiere_sortierbare_id(wert: Any) -> Optional[int]:
    if wert is None or isinstance(wert, bool):
        return None

    if isinstance(wert, int):
        return wert

    if isinstance(wert, float):
        if wert.is_integer():
            return int(wert)
        return None

    if isinstance(wert, str):
        text = wert.strip()
        if not text:
            return None
        if not INTEGER_STRING_PATTERN.fullmatch(text):
            return None
        try:
            return int(text)
        except ValueError:
            return None

    return None


def _ermittle_numeric_pk_kandidat(arr: Sequence[Dict[str, Any]]) -> Optional[str]:
    if not arr:
        return None

    erstes_element = arr[0]
    if not isinstance(erstes_element, dict) or not erstes_element:
        return None

    erster_id_key: Optional[str] = None
    for key in erstes_element.keys():
        if _ist_id_schluessel(key):
            erster_id_key = str(key)
            break

    if erster_id_key is None:
        return None

    for element in arr:
        if not isinstance(element, dict):
            return None
        if erster_id_key not in element:
            return None
        if _konvertiere_sortierbare_id(element.get(erster_id_key)) is None:
            return None

    return erster_id_key


def _ist_numerische_folge_aufsteigend(werte: Sequence[int]) -> bool:
    if not werte:
        return True

    vorher = werte[0]
    for aktuell in werte[1:]:
        if aktuell < vorher:
            return False
        vorher = aktuell

    return True


def _numeric_pk_sorter(
    eintrag: Tuple[Dict[str, Any], int, int],
) -> Tuple[int, int]:
    return (eintrag[1], eintrag[2])


def _pfad_zu_text(pfad: Sequence[str]) -> str:
    if not pfad:
        return "<root>"

    teile: List[str] = []
    for teil in pfad:
        text = str(teil)
        if text.isdigit():
            teile.append("[" + text + "]")
            continue

        if not teile:
            teile.append(text)
            continue

        teile.append("." + text)

    return "".join(teile)


def _ist_tabellenpfad(pfad: Sequence[str]) -> bool:
    for teil in pfad:
        if str(teil).isdigit():
            return False
    return True


def _neue_numeric_sort_statistik() -> Dict[str, Any]:
    return {
        "listen_geprueft": 0,
        "objektlisten_geprueft": 0,
        "sortierbare_objektlisten": 0,
        "objektlisten_sortiert": 0,
        "objektlisten_bereits_sortiert": 0,
        "objektlisten_ohne_numeric_pk": 0,
        "datensaetze_sortiert": 0,
        "sortierte_pfade": [],
    }


def _rekursiv_numeric_pk_sortieren(
    wert: Any,
    pfad: Tuple[str, ...],
    statistik: Dict[str, Any],
) -> Any:
    if isinstance(wert, dict):
        neu: Dict[str, Any] = {}
        for key, value in wert.items():
            neu[key] = _rekursiv_numeric_pk_sortieren(value, pfad + (str(key),), statistik)
        return neu

    if isinstance(wert, list):
        statistik["listen_geprueft"] += 1

        verarbeitete_liste: List[Any] = []
        for index, element in enumerate(wert):
            if isinstance(element, (dict, list)):
                verarbeitete_liste.append(
                    _rekursiv_numeric_pk_sortieren(element, pfad + (str(index),), statistik)
                )
            else:
                verarbeitete_liste.append(element)

        if (
            verarbeitete_liste
            and all(isinstance(element, dict) for element in verarbeitete_liste)
            and _ist_tabellenpfad(pfad)
        ):
            statistik["objektlisten_geprueft"] += 1
            kandidat = _ermittle_numeric_pk_kandidat(verarbeitete_liste)

            if kandidat is None:
                statistik["objektlisten_ohne_numeric_pk"] += 1
                return verarbeitete_liste

            statistik["sortierbare_objektlisten"] += 1
            numerische_ids = [
                _konvertiere_sortierbare_id(element[kandidat]) for element in verarbeitete_liste
            ]

            if not all(wert is not None for wert in numerische_ids):
                statistik["objektlisten_ohne_numeric_pk"] += 1
                return verarbeitete_liste

            int_ids = [int(wert) for wert in numerische_ids if wert is not None]
            if _ist_numerische_folge_aufsteigend(int_ids):
                statistik["objektlisten_bereits_sortiert"] += 1
                return verarbeitete_liste

            kombiniert = list(zip(verarbeitete_liste, int_ids, range(len(verarbeitete_liste))))
            sortiert = sorted(kombiniert, key=_numeric_pk_sorter)
            statistik["objektlisten_sortiert"] += 1
            statistik["datensaetze_sortiert"] += len(verarbeitete_liste)
            statistik["sortierte_pfade"].append(
                {
                    "pfad": _pfad_zu_text(pfad),
                    "pk": kandidat,
                    "anzahl": len(verarbeitete_liste),
                }
            )
            return [element for element, _, _ in sortiert]

        return verarbeitete_liste

    return wert


def json_reparieren(json_text: str) -> str:
    """
    Repariert einige häufige JSON-Fehler (non-destructive best-effort).
    """
    json_text = re.sub(r",\s*}", "}", json_text)
    json_text = re.sub(r",\s*]", "]", json_text)

    def _ersatz(match: re.Match[str]) -> str:
        return '"' + match.group(1) + '"'

    json_text = re.sub(r"'([^']*)'", _ersatz, json_text)
    json_text = re.sub(r'([,\{\s])(\w+):\s*', r'\1"\2": ', json_text)
    json_text = re.sub(r'""([^"]+)""', r'"\1"', json_text)
    json_text = re.sub(r"//.*$", "", json_text, flags=re.MULTILINE)
    json_text = re.sub(r"/\*.*?\*/", "", json_text, flags=re.DOTALL)
    return json_text


def _lade_json_aus_text(json_text: str) -> Any:
    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        repariert = json_reparieren(json_text)
        return json.loads(repariert)


def _lade_json(dateiname: str) -> Any:
    with open(dateiname, "r", encoding="utf-8") as datei:
        raw = datei.read()
    return _lade_json_aus_text(raw)


def _append_candidate_order(
    orders: Dict[Tuple[str, ...], List[List[str]]],
    pfad: Tuple[str, ...],
    neue_keys: Sequence[str],
) -> None:
    if pfad not in orders:
        orders[pfad] = []

    kandidat = list(neue_keys)
    bestehende = orders[pfad]
    for vorhandener_kandidat in bestehende:
        if vorhandener_kandidat == kandidat:
            return

    bestehende.append(kandidat)


def _sammle_referenz_key_orders(
    wert: Any,
    pfad: Tuple[str, ...],
    orders: Dict[Tuple[str, ...], List[List[str]]],
) -> None:
    if isinstance(wert, dict):
        keys = list(wert.keys())
        _append_candidate_order(orders, pfad, keys)
        for key, value in wert.items():
            _sammle_referenz_key_orders(value, pfad + (str(key),), orders)
        return

    if isinstance(wert, list):
        wildcard_path = pfad + ("[]",)
        for element in wert:
            if isinstance(element, (dict, list)):
                _sammle_referenz_key_orders(element, wildcard_path, orders)


def _reorder_dict_with_order(wert: Dict[str, Any], key_order: Sequence[str]) -> Dict[str, Any]:
    neue_reihenfolge: List[str] = []
    vorhanden = set(wert.keys())

    for key in key_order:
        if key in vorhanden:
            neue_reihenfolge.append(key)

    for key in wert.keys():
        if key not in neue_reihenfolge:
            neue_reihenfolge.append(key)

    return {key: wert[key] for key in neue_reihenfolge}


def _waehle_passende_referenzreihenfolge(
    aktuelle_keys: Sequence[str],
    kandidaten: Sequence[Sequence[str]],
) -> Optional[List[str]]:
    if not kandidaten:
        return None

    aktuelle_liste = list(aktuelle_keys)
    aktuelle_menge = set(aktuelle_liste)

    bester_kandidat: Optional[List[str]] = None
    bester_score: Optional[Tuple[int, int, int]] = None

    for kandidat in kandidaten:
        kandidat_liste = list(kandidat)
        kandidat_menge = set(kandidat_liste)
        overlap = len(aktuelle_menge.intersection(kandidat_menge))
        if overlap == 0:
            continue

        gleicher_schluesselsatz = 1 if aktuelle_menge == kandidat_menge else 0
        gleiche_reihenfolge = 1 if aktuelle_liste == kandidat_liste else 0
        score = (gleiche_reihenfolge, gleicher_schluesselsatz, overlap)

        if bester_score is None or score > bester_score:
            bester_score = score
            bester_kandidat = kandidat_liste

    if bester_kandidat is not None:
        return bester_kandidat

    return None


def _reorder_by_reference_orders(
    wert: Any,
    orders: Dict[Tuple[str, ...], List[List[str]]],
    pfad: Tuple[str, ...],
) -> Any:
    if isinstance(wert, dict):
        kandidaten = orders.get(pfad, [])
        key_order = _waehle_passende_referenzreihenfolge(list(wert.keys()), kandidaten)
        aktuell = wert
        if key_order:
            aktuell = _reorder_dict_with_order(wert, key_order)

        neu: Dict[str, Any] = {}
        for key, value in aktuell.items():
            neu[key] = _reorder_by_reference_orders(value, orders, pfad + (str(key),))
        return neu

    if isinstance(wert, list):
        wildcard_path = pfad + ("[]",)
        neu_liste: List[Any] = []
        for element in wert:
            if isinstance(element, (dict, list)):
                neu_liste.append(_reorder_by_reference_orders(element, orders, wildcard_path))
            else:
                neu_liste.append(element)
        return neu_liste

    return wert


def _ermittle_tabellengroesse(wert: Any) -> int:
    if isinstance(wert, (list, dict, tuple, set, str, bytes)):
        return len(wert)
    return 1


def _sortiere_top_level_alpha(daten: Any) -> Any:
    if not isinstance(daten, dict):
        return daten

    sorted_keys = sorted(daten.keys(), key=partial(str.lower))
    return {key: daten[key] for key in sorted_keys}


def _sortiere_top_level_table_size(daten: Any) -> Any:
    if not isinstance(daten, dict):
        return daten

    keys = list(daten.keys())
    index_map = {key: index for index, key in enumerate(keys)}
    sorted_keys = sorted(
        keys,
        key=partial(
            _key_table_size_sorter,
            daten=daten,
            index_map=index_map,
        ),
    )
    return {key: daten[key] for key in sorted_keys}


def _key_table_size_sorter(key: str, daten: Dict[str, Any], index_map: Dict[str, int]) -> Tuple[int, int, str]:
    return (-_ermittle_tabellengroesse(daten[key]), index_map[key], key)


def _sortiere_top_level_reference(daten: Any, referenz: Any) -> Any:
    if not isinstance(daten, dict):
        return daten

    if not isinstance(referenz, dict):
        return daten

    referenz_keys = list(referenz.keys())
    return _reorder_dict_with_order(daten, referenz_keys)


def finde_referenz_sort_datei(input_datei: str, explizite_datei: Optional[str] = None) -> Optional[str]:
    if explizite_datei:
        if os.path.isfile(explizite_datei):
            return explizite_datei
        return None

    input_abspath = os.path.abspath(input_datei)
    input_dir = os.path.dirname(input_abspath) or "."
    input_base = os.path.basename(input_abspath)

    if input_base in DEFAULT_REFERENCE_CANDIDATES and os.path.isfile(input_abspath):
        return input_abspath

    kandidaten: List[str] = []
    for name in DEFAULT_REFERENCE_CANDIDATES:
        kandidaten.append(os.path.join(input_dir, name))
        kandidaten.append(name)

    bereits_geprueft = set()
    for kandidat in kandidaten:
        absolut = os.path.abspath(kandidat)
        if absolut in bereits_geprueft:
            continue
        bereits_geprueft.add(absolut)
        if os.path.isfile(absolut):
            return absolut

    return None


def normalisiere_sort_modus(sort_modus: Optional[str]) -> str:
    if sort_modus is None:
        return DEFAULT_SORT_MODE

    key = str(sort_modus).strip().lower()
    if key in SORT_MODE_ALIASES:
        return SORT_MODE_ALIASES[key]

    erlaubte_werte = ", ".join(sorted(set(SORT_MODE_ALIASES.values())))
    raise argparse.ArgumentTypeError(
        "Ungültiger Sortiermodus '" + str(sort_modus) + "'. Erlaubt: " + erlaubte_werte
    )


def sortiere_json_daten(
    daten: Any,
    sort_modus: Optional[str] = None,
    referenz_daten: Optional[Any] = None,
) -> Tuple[Any, Dict[str, Any]]:
    modus = normalisiere_sort_modus(sort_modus)
    original_signatur = _json_dumps_compact(daten)

    if modus == "none":
        return daten, {
            "sort_modus": modus,
            "bereits_korrekt_sortiert": True,
            "sortierung_angewendet": False,
        }

    if modus == "numeric_pk":
        statistik = _neue_numeric_sort_statistik()
        neu = _rekursiv_numeric_pk_sortieren(daten, tuple(), statistik)
        neue_signatur = _json_dumps_compact(neu)
        bereits_sortiert = original_signatur == neue_signatur
        statistik.update(
            {
                "sort_modus": modus,
                "bereits_korrekt_sortiert": bereits_sortiert,
                "sortierung_angewendet": not bereits_sortiert,
            }
        )
        return neu, statistik

    if modus == "alpha":
        neu = _sortiere_top_level_alpha(daten)
    elif modus == "table_size":
        neu = _sortiere_top_level_table_size(daten)
    elif modus == "reference":
        if referenz_daten is None:
            raise ValueError(
                "Sortiermodus 'reference' benötigt eine Referenzdatei. "
                "Nutze --sort-reference DATEI oder wähle -S none/numeric_pk/alpha/table_size."
            )

        top_sorted = _sortiere_top_level_reference(daten, referenz_daten)
        orders: Dict[Tuple[str, ...], List[List[str]]] = {}
        _sammle_referenz_key_orders(referenz_daten, tuple(), orders)
        neu = _reorder_by_reference_orders(top_sorted, orders, tuple())
    else:
        raise ValueError("Nicht unterstützter Sortiermodus: " + modus)

    neue_signatur = _json_dumps_compact(neu)
    bereits_sortiert = original_signatur == neue_signatur
    return neu, {
        "sort_modus": modus,
        "bereits_korrekt_sortiert": bereits_sortiert,
        "sortierung_angewendet": not bereits_sortiert,
    }


def _build_format_context(einrueckung: int) -> Dict[str, Any]:
    return {
        "einrueckung": int(einrueckung),
        "leerzeichen": " " * int(einrueckung),
        "dump_inline": _dump_inline_factory(),
        "top_spacing": dict(DEFAULT_TOP_SPACING),
    }


def objekt_zu_einzeiler(obj: Union[Dict[str, Any], List[Any], Any], kontext: Dict[str, Any]) -> str:
    return kontext["dump_inline"](obj)


def formatiere_wert(
    wert: Any,
    ebene: int,
    ist_letztes: bool,
    pfad: List[str],
    kontext: Dict[str, Any],
) -> str:
    if isinstance(wert, dict):
        return formatiere_dict(wert, ebene, ist_letztes, pfad, kontext)
    if isinstance(wert, list):
        return formatiere_array(wert, ebene, ist_letztes, pfad, kontext)
    return f"{json.dumps(wert, ensure_ascii=False)}{'' if ist_letztes else ','}"


def formatiere_dict(
    obj: Dict[str, Any],
    ebene: int,
    ist_letztes: bool,
    pfad: List[str],
    kontext: Dict[str, Any],
) -> str:
    if not obj:
        return f"{{}}{'' if ist_letztes else ','}"

    einr = kontext["leerzeichen"] * ebene
    einr_next = kontext["leerzeichen"] * (ebene + 1)

    zeilen: List[str] = ["{"]
    keys = list(obj.keys())

    for index, key in enumerate(keys):
        value = obj[key]
        is_last_key = index == len(keys) - 1
        key_json = json.dumps(key, ensure_ascii=False)

        if ebene == 0 and index > 0:
            gap = int(kontext["top_spacing"].get(str(key), 0))
            for _ in range(gap):
                zeilen.append("")

        if isinstance(value, (dict, list)):
            formatiert = formatiere_wert(value, ebene + 1, is_last_key, pfad + [str(key)], kontext)
            zeilen.append(f"{einr_next}{key_json}: {formatiert}")
        else:
            value_json = json.dumps(value, ensure_ascii=False)
            zeilen.append(f"{einr_next}{key_json}: {value_json}{'' if is_last_key else ','}")

    zeilen.append(f"{einr}}}{'' if ist_letztes else ','}")
    return "\n".join(zeilen)


def formatiere_array(
    arr: List[Any],
    ebene: int,
    ist_letztes: bool,
    pfad: List[str],
    kontext: Dict[str, Any],
) -> str:
    if not arr:
        return f"[]{'' if ist_letztes else ','}"

    einr = kontext["leerzeichen"] * ebene
    einr_next = kontext["leerzeichen"] * (ebene + 1)
    zeilen: List[str] = ["["]

    alle_dicts = all(isinstance(element, dict) for element in arr)
    expand_elements = _ist_dynamic_rule_sets_pfad(pfad) or _ist_automation_process_state_mappings_pfad(pfad)

    if alle_dicts and not expand_elements:
        for index, element in enumerate(arr):
            last = index == len(arr) - 1
            one_line = objekt_zu_einzeiler(element, kontext)
            zeilen.append(f"{einr_next}{one_line}{'' if last else ','}")
        zeilen.append(f"{einr}]{'' if ist_letztes else ','}")
        return "\n".join(zeilen)

    for index, element in enumerate(arr):
        last = index == len(arr) - 1
        formatiert = formatiere_wert(element, ebene + 1, last, pfad + [str(index)], kontext)
        zeilen.append(f"{einr_next}{formatiert}")
    zeilen.append(f"{einr}]{'' if ist_letztes else ','}")
    return "\n".join(zeilen)


def formatiere(json_daten: Union[str, Dict[str, Any], List[Any]], einrueckung: int = 2) -> str:
    if isinstance(json_daten, str):
        daten = _lade_json_aus_text(json_daten)
    else:
        daten = json_daten

    kontext = _build_format_context(einrueckung=einrueckung)
    return formatiere_wert(daten, 0, True, [], kontext)


def _schreibe_datei_atomic(dateiname: str, inhalt: str) -> None:
    ziel = os.path.abspath(dateiname)
    ziel_dir = os.path.dirname(ziel) or "."
    ziel_base = os.path.basename(ziel)

    fd, tmp_path = tempfile.mkstemp(prefix=ziel_base + ".tmp.", dir=ziel_dir, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as datei:
            datei.write(inhalt)
            if not inhalt.endswith("\n"):
                datei.write("\n")

        if os.path.exists(ziel):
            try:
                stat_info = os.stat(ziel)
                os.chmod(tmp_path, stat_info.st_mode)
            except Exception:
                pass

        os.replace(tmp_path, ziel)
        tmp_path = None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def _finde_default_test_file() -> Optional[str]:
    for name in DEFAULT_REFERENCE_CANDIDATES:
        if os.path.isfile(name):
            return name
    return None


def _ausgabe_status(sort_info: Dict[str, Any], ziel_stdout_json: bool) -> None:
    teile = [
        "Sortiermodus: " + str(sort_info["sort_modus"]),
        "Bereits korrekt sortiert: " + ("ja" if sort_info["bereits_korrekt_sortiert"] else "nein"),
    ]

    if "sortierbare_objektlisten" in sort_info:
        teile.append("Sortierbare Objektlisten: " + str(sort_info["sortierbare_objektlisten"]))
    if "objektlisten_sortiert" in sort_info:
        teile.append("Neu sortiert: " + str(sort_info["objektlisten_sortiert"]))
    if "objektlisten_bereits_sortiert" in sort_info:
        teile.append("Bereits sortiert: " + str(sort_info["objektlisten_bereits_sortiert"]))

    meldung = " | ".join(teile)
    stream = sys.stderr if ziel_stdout_json else sys.stdout
    print(meldung, file=stream)


def datei_verarbeiten(
    dateiname: str,
    ausgabe_datei: Optional[str] = None,
    einrueckung: int = 2,
    nur_automation_process_state_mappings: bool = False,
    validiere: bool = False,
    inplace: bool = False,
    sort_modus: Optional[str] = None,
    sort_reference_datei: Optional[str] = None,
) -> None:
    try:
        with open(dateiname, "r", encoding="utf-8") as datei:
            raw = datei.read()

        daten = _lade_json_aus_text(raw)

        if nur_automation_process_state_mappings:
            if not isinstance(daten, dict):
                raise ValueError(
                    "Snippet-Modus erwartet ein JSON-Objekt auf Top-Level."
                )
            daten = {
                "automation_process_state_mappings": daten.get("automation_process_state_mappings")
            }

        referenz_daten = None
        modus = normalisiere_sort_modus(sort_modus)
        if modus == "reference":
            referenz_pfad = finde_referenz_sort_datei(dateiname, explizite_datei=sort_reference_datei)
            if not referenz_pfad:
                raise ValueError(
                    "Keine Referenzdatei für Sortiermodus 'reference' gefunden. "
                    "Erwartet z.B. '___device_state_data.json' im gleichen Verzeichnis oder per --sort-reference."
                )
            referenz_daten = _lade_json(referenz_pfad)

        sortierte_daten, sort_info = sortiere_json_daten(
            daten,
            sort_modus=modus,
            referenz_daten=referenz_daten,
        )

        out = formatiere(sortierte_daten, einrueckung=einrueckung)

        if validiere:
            json.loads(out)

        ziel_datei = ausgabe_datei
        if inplace:
            ziel_datei = dateiname

        ziel_stdout_json = ziel_datei is None
        _ausgabe_status(sort_info, ziel_stdout_json=ziel_stdout_json)

        if ziel_datei:
            if nur_automation_process_state_mappings and os.path.abspath(ziel_datei) == os.path.abspath(dateiname):
                raise ValueError(
                    "Inplace/Output auf Eingabedatei ist im Snippet-Modus gesperrt (würde die Datei überschreiben)."
                )

            _schreibe_datei_atomic(ziel_datei, out)
            if os.path.abspath(ziel_datei) == os.path.abspath(dateiname):
                print("Formatierte JSON wurde in der Eingabedatei aktualisiert.")
            else:
                print("Formatierte JSON wurde in '" + ziel_datei + "' gespeichert.")
            return

        print(out)
    except FileNotFoundError:
        print("Fehler: Datei '" + dateiname + "' nicht gefunden.")
    except Exception as exc:
        print("Fehler beim Verarbeiten: " + str(exc))


def main() -> None:
    parser = argparse.ArgumentParser(
        prog=os.path.basename(sys.argv[0]),
        description=(
            "JSON-Formatierer mit Einzeiler-Arrays (ein Element pro Zeile), "
            "Spezialfällen für dynamic_rule_engine.rule_sets und "
            "automation_process_state_mappings sowie optionaler Sortierung."
        ),
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        help="Eingabe-JSON-Datei. Wenn weggelassen, wird automatisch nach '___device_state_data.json' gesucht.",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default=None,
        help="Optional: Ausgabedatei. Wenn nicht gesetzt, wird auf stdout ausgegeben.",
    )
    parser.add_argument(
        "--inplace",
        "-i",
        dest="inplace",
        action="store_true",
        help="Schreibt die formatierte Ausgabe atomar zurück in die Eingabedatei (sicheres In-Place-Update).",
    )
    parser.add_argument(
        "--indent",
        dest="indent",
        type=int,
        default=2,
        help="Einrückung pro Ebene (Default: 2). Für 4 Spaces: --indent 4",
    )
    parser.add_argument(
        "--snippet-automation-process-state-mappings",
        dest="snippet_automation",
        action="store_true",
        help="Nur den Abschnitt 'automation_process_state_mappings' ausgeben (schneller Sichttest).",
    )
    parser.add_argument(
        "--validate",
        dest="validate",
        action="store_true",
        help="Nach dem Formatieren die Ausgabe erneut parsen (Gültigkeit prüfen).",
    )
    parser.add_argument(
        "--sort",
        "-S",
        dest="sort_modus",
        type=normalisiere_sort_modus,
        default=DEFAULT_SORT_MODE,
        help=(
            "Sortiermodus vor der Formatierung. "
            "Erlaubt: numeric_pk (Default), reference, alpha, table_size, none"
        ),
    )
    parser.add_argument(
        "--sort-reference",
        dest="sort_reference_datei",
        default=None,
        help=(
            "Explizite Referenzdatei für -S reference. "
            "Wenn nicht gesetzt, wird automatisch z.B. '___device_state_data.json' gesucht."
        ),
    )

    args = parser.parse_args()

    inp = args.input
    if not inp:
        inp = _finde_default_test_file()
        if not inp:
            parser.print_help()
            sys.exit(1)

    if args.inplace and args.output and os.path.abspath(args.output) != os.path.abspath(inp):
        print("Fehler: Entweder --inplace ODER eine separate output-Datei angeben (nicht beides).")
        sys.exit(2)

    datei_verarbeiten(
        inp,
        args.output,
        einrueckung=args.indent,
        nur_automation_process_state_mappings=bool(args.snippet_automation),
        validiere=bool(args.validate),
        inplace=bool(args.inplace),
        sort_modus=str(args.sort_modus),
        sort_reference_datei=args.sort_reference_datei,
    )


if __name__ == "__main__":
    main()
