from typing import Any, Optional, Tuple
# from .Process import BaseProcess
from pathlib import Path


class Absent:
    """ 
    Used to indictate the absence of a runtime input. Useful when None is a
    valid value for an argument.
    """
    def __repr__(self) -> str:
        return "Absent(None)"


# def get_or_default(d: dict, default_value: Any) -> Any:
#     if not isinstance(d, dict):
#         raise TypeError(f"Expected dict, but has type {type(d)}")
#     if default_value in d:
#         return d[default_value]
#     return default_value

class FileObject:
    """
    Object that stores path properties as strings.
    Available properties: 
        path     : path/to/file.ext,
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