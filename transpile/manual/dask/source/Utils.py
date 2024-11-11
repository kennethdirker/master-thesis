# Standard modules
import typing

def retrieve_attr(self, obj: object, attr: str) -> Any | None:
    """
    Returns the object attribute. Raises an exception if it doesn't exist.
    """
    if hasattr(obj, attr):
        return getattr(obj, attr)
    else:
        raise Exception(f"Object {obj} does not have attribute {attr}.")