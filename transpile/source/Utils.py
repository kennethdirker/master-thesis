# Standard imports
from typing import Any

class NoneType:
    """ 
    Type representing missing data. 
    Use instead of 'None' when data might contain 'None' values.
    """
    pass

    
def getattr_or_nonetype(obj: object, attr: str) -> Any:
    """
    Returns the object attribute or NoneType if the attribute doesn't exist.
    Note: Use when attribute retrieval is optional, otherwise use getattr.
    """
    if hasattr(obj, attr):
        return getattr(obj, attr)
    return NoneType
