from typing import Any, Optional, Tuple
# from .Process import BaseProcess
# from pathlib import Path


class Absent:
    """ 
    Used to indictate the absence of a runtime input. Useful when None is a
    valid value for an argument.
    """
    pass


# def get_or_default(d: dict, default_value: Any) -> Any:
#     if not isinstance(d, dict):
#         raise TypeError(f"Expected dict, but has type {type(d)}")
#     if default_value in d:
#         return d[default_value]
#     return default_value
