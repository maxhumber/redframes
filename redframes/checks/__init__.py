from typing import Any

def enforce(argument: Any, types: set[Any]):
    optional = (None in types)
    just_types = types.difference({None})
    checks = [isinstance(argument, t) for t in just_types]
    if optional:
        checks += [argument == None]
    if not any(checks):
        str_types = " | ".join([t.__name__ for t in just_types])
        if optional: 
            str_types += " | None"
        raise TypeError(f"must be {str_types}")