from typing import Any
# from pathlib import Path


class Absent:
    """ Used to indictate the absence of a runtime input. """
    pass

def get_or_default(d: dict, default_value: Any) -> Any:
    if not isinstance(d, dict):
        raise TypeError(f"Received {type(d)}, where 'dict' was expected")
    if default_value in d:
        return d[default_value]
    return default_value


    
