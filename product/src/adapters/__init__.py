# -*- coding: utf-8 -*-

# src/adapters/__init__.py

"""Adapter-Package: Alle Feldbus-Adapter + SPS-Schnittstellen.

Phase 3 komplett:
    fieldbus/
        _modbus_tcp_adapter.py     ✓ Modbus TCP/UDP (pymodbus)
        _profinet_rt_adapter.py    ✓ PROFINET RT (python-snap7)
        _profibus_gw_adapter.py    ✓ PROFIBUS DP Gateway (→ Modbus TCP)
        _ethernet_ip_adapter.py    ✓ EtherNet/IP CIP (pycomm3)
        _knx_ip_adapter.py         ✓ KNX IP (xknx)
    plc/
        _plc_modbus_adapter.py     ✓ SPS über Modbus TCP
        _plc_opcua_adapter.py      ✓ SPS über OPC-UA (asyncua)
"""

import logging
from functools import partial

logger = logging.getLogger(__name__)


def register_all_adapters():
    """Registriert alle verfügbaren Adapter-Factories.

    Nur Adapter mit installierten Abhängigkeiten werden registriert.
    Fehlende Libraries → Adapter wird übersprungen (kein Crash).
    """
    try:
        from src.adapters._connection_registry import register_adapter_factory
    except ImportError:
        from _connection_registry import register_adapter_factory

    registered = []

    # ==== Feldbus-Adapter ====

    # Modbus TCP (pymodbus)
    try:
        from src.adapters.fieldbus._modbus_tcp_adapter import create_modbus_tcp_adapter
        register_adapter_factory("modbus_tcp", create_modbus_tcp_adapter)
        registered.append("modbus_tcp")
    except ImportError:
        try:
            from fieldbus._modbus_tcp_adapter import create_modbus_tcp_adapter
            register_adapter_factory("modbus_tcp", create_modbus_tcp_adapter)
            registered.append("modbus_tcp")
        except ImportError:
            logger.info("Adapter nicht verfügbar: modbus_tcp (pymodbus fehlt)")

    # PROFINET RT (python-snap7)
    try:
        from src.adapters.fieldbus._profinet_rt_adapter import create_profinet_rt_adapter
        register_adapter_factory("profinet_rt", create_profinet_rt_adapter)
        registered.append("profinet_rt")
    except ImportError:
        try:
            from fieldbus._profinet_rt_adapter import create_profinet_rt_adapter
            register_adapter_factory("profinet_rt", create_profinet_rt_adapter)
            registered.append("profinet_rt")
        except ImportError:
            logger.info("Adapter nicht verfügbar: profinet_rt (python-snap7 fehlt)")

    # PROFIBUS Gateway (→ Modbus TCP)
    try:
        from src.adapters.fieldbus._profibus_gw_adapter import create_profibus_gw_adapter
        register_adapter_factory("profibus_gw", create_profibus_gw_adapter)
        registered.append("profibus_gw")
    except ImportError:
        try:
            from fieldbus._profibus_gw_adapter import create_profibus_gw_adapter
            register_adapter_factory("profibus_gw", create_profibus_gw_adapter)
            registered.append("profibus_gw")
        except ImportError:
            logger.info("Adapter nicht verfügbar: profibus_gw")

    # EtherNet/IP (pycomm3)
    try:
        from src.adapters.fieldbus._ethernet_ip_adapter import create_ethernet_ip_adapter
        register_adapter_factory("ethernet_ip", create_ethernet_ip_adapter)
        registered.append("ethernet_ip")
    except ImportError:
        try:
            from fieldbus._ethernet_ip_adapter import create_ethernet_ip_adapter
            register_adapter_factory("ethernet_ip", create_ethernet_ip_adapter)
            registered.append("ethernet_ip")
        except ImportError:
            logger.info("Adapter nicht verfügbar: ethernet_ip (pycomm3 fehlt)")

    # KNX IP (xknx)
    try:
        from src.adapters.fieldbus._knx_ip_adapter import create_knx_ip_adapter
        register_adapter_factory("knx_ip", create_knx_ip_adapter)
        registered.append("knx_ip")
    except ImportError:
        try:
            from fieldbus._knx_ip_adapter import create_knx_ip_adapter
            register_adapter_factory("knx_ip", create_knx_ip_adapter)
            registered.append("knx_ip")
        except ImportError:
            logger.info("Adapter nicht verfügbar: knx_ip (xknx fehlt)")

    # ==== SPS-Adapter ====

    # PLC Modbus
    try:
        from src.adapters.plc._plc_modbus_adapter import create_plc_modbus_adapter
        register_adapter_factory("plc_modbus", create_plc_modbus_adapter)
        registered.append("plc_modbus")
    except ImportError:
        try:
            from plc._plc_modbus_adapter import create_plc_modbus_adapter
            register_adapter_factory("plc_modbus", create_plc_modbus_adapter)
            registered.append("plc_modbus")
        except ImportError:
            logger.info("Adapter nicht verfügbar: plc_modbus")

    # PLC OPC-UA (asyncua)
    try:
        from src.adapters.plc._plc_opcua_adapter import create_plc_opcua_adapter
        register_adapter_factory("plc_opcua", create_plc_opcua_adapter)
        registered.append("plc_opcua")
    except ImportError:
        try:
            from plc._plc_opcua_adapter import create_plc_opcua_adapter
            register_adapter_factory("plc_opcua", create_plc_opcua_adapter)
            registered.append("plc_opcua")
        except ImportError:
            logger.info("Adapter nicht verfügbar: plc_opcua (asyncua fehlt)")

    logger.info("Adapter registriert (%d): %s", len(registered),
                ", ".join(registered) if registered else "KEINE")
    return registered
