from typing import Any, Callable

LoadPort = Callable[[str], Any]
DumpPort = Callable[[str, Any], None]
ClockPort = Callable[[], str]
IdPort = Callable[[], str]
AuditPort = Callable[[str, Any], None]
