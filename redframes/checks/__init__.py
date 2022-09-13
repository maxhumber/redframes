from __future__ import annotations

from typing import Any


def enforce(argument: Any, against: type | set[type | None]):
    if isinstance(against, set):
        if len(against) == 0:
            against = {against}  # type: ignore
    if not isinstance(against, set):
        against = {against}
    optional = None in against
    just_types = against.difference({None})
    checks = [isinstance(argument, t) for t in just_types]  # type: ignore
    if optional:
        checks += [argument == None]
    if not any(checks):
        str_types = " | ".join([t.__name__ for t in just_types])  # type: ignore
        if optional:
            str_types += " | None"
        raise TypeError(f"must be {str_types}")
