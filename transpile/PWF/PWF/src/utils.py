from typing import Any, List, Optional, Type, Sequence, Mapping
from types import NoneType
# from .Process import BaseProcess
from pathlib import Path

class Absent:
    """ 
    Used to indictate the absence of a runtime input. Useful when None is a
    valid value for an argument.
    """

    def __init__(self, string: str = "None") -> None:
        self.string = string

    def __repr__(self) -> str:
        return f'Absent("{self.string}")'


# def get_or_default(d: dict, default_value: Any) -> Any:
#     if not isinstance(d, dict):
#         raise TypeError(f"Expected dict, but has type {type(d)}")
#     if default_value in d:
#         return d[default_value]
#     return default_value
class NestedObject:
    pass

def dict_to_obj(d: dict) -> Any:
    """
    Convert a dictionary to an object with attributes.
    """
    def helper(obj, d: dict):
        for key, value in d.items():
            if type(value) is dict:
                setattr(obj, key, NestedObject())
                helper(getattr(obj, key), value)
            else:
                setattr(obj, key, value)

    obj = NestedObject()
    helper(obj, d)
    return obj


# def print_obj(obj: object, indent: int = 0):
#     for key in dir(obj):
#         if not key.startswith("__"):
#             value = getattr(obj, key)
#             if type(value) is NestedObject:
#                 print("\t" * indent, f"{key}:")
#                 print_obj(value, indent + 1)
#             else:
#                 print("\t" * indent, f"{key}: {value}")


def print_obj(obj: object, indent: int = 0, filter: Optional[Sequence] = None):
    """
    Pretty print a CWL object recursively.
    """
    if hasattr(obj, "__dict__"):
        for attr, value in obj.__dict__.items():
            if filter and attr in filter:
                continue

            print("\t" * indent + attr)
            if isinstance(value, Sequence) and not isinstance(value, str):
                for elem in value:
                    print_obj(elem, indent + 1, filter)
            elif isinstance(value, Mapping):
                for k, v in value.items():
                    print("\t" * indent + k + ":")
                    print_obj(v, indent + 2, filter)
            else:
                print_obj(value, indent + 1, filter)
    elif obj:
        print("\t" * indent + str(obj))


class FileObject:
    """
    Object that stores path properties as strings.
    Available properties: 
        path     : absolute/path/to/file.ext,
        basename : file.ext,
        dirname  : path/to,
        nameroot : file,
        nameext  : .ext
    """
    
    path: str = ""
    basename: str = ""
    dirname: str = ""
    nameroot: str = ""
    nameext: str = ""
    
    def __init__(self, file_path: str):
        path: Path = Path(file_path).resolve()
        self.path = str(path)
        self.basename = path.name
        self.dirname = str(path.parent)
        self.nameroot = path.stem
        self.nameext = path.suffix

    def __str__(self) -> str:
        return self.path
    
    def __repr__(self) -> str:
        return f"FileObject({self.path})"