# Standard modules
# import typing
from typing import Any

def retrieve_attr(obj: object, attr: str) -> Any:
    """
    Returns the object attribute. Raises an exception if it doesn't exist.
    """
    if hasattr(obj, attr):
        return getattr(obj, attr)
    else:
        raise Exception(f"Object {obj} does not have attribute {attr}.")