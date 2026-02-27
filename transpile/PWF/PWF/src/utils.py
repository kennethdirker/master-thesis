from __future__ import annotations

from typing import Any, List, Optional, Type, Sequence, Mapping, Union
from types import NoneType
from pathlib import Path
from copy import deepcopy

class Absent:
    """ 
    Used to indictate the absence of a runtime input. Useful when None is a
    valid value for an argument.
    """

    string: str
    value = None
    cwltype: str = "null"

    def __init__(self, string: str = "None") -> None:
        self.string = string

    def __repr__(self) -> str:
        return f'Absent("{self.string}")'


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


def pretty_print_dict(d, indent=0):
    res = ""
    for k, v in d.items():
        res += "\t"*indent + str(k) + "\n"
        if isinstance(v, dict):
            res += pretty_print_dict(v, indent+1)
        else:
            res += "\t"*(indent+1) + str(v) + "\n"
    return res


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
    
    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: str
    contents: str
    size: int
    # writable: bool = True
    # MAX_SIZE: int = 64000
    
    def __init__(self, file_path: str | Path | FileObject):
        if isinstance(file_path, str):
            file_path = Path(file_path)
        if isinstance(file_path, Path):
            # path: Path = file_path.resolve() < BUG dont use
            # pathlib.Path.resolve resolves symlinks, which is unwanted
            # behaviour, as we are sometimes pointing to symlinks. Normalizing
            # the parent and adding the name part circumvents this.
            path: Path = file_path.parent.resolve() / file_path.name
            
            self.path = str(path)
            self.basename = path.name
            self.dirname = str(path.parent)
            self.nameroot = path.stem
            self.nameext = path.suffix
        elif isinstance(file_path, FileObject):
            self.path = file_path.path
            self.basename = file_path.basename
            self.dirname = file_path.dirname
            self.nameroot = file_path.nameroot
            self.nameext = file_path.nameext
            self.contents = file_path.contents
            self.size = file_path.size

        else:
            raise Exception(f"FileObject expects 'str' | 'Path' | 'FileObject', but found '{type(file_path)}'")

    def resolve(self) -> FileObject:
        return FileObject(Path(self.path).resolve())
    
    def resolve_as_str(self) -> str:
        return str(Path(self.path).resolve())
    
    def create(self, contents: Optional[str] = None) -> None:
        with open(self.path, "w") as f:
            if contents:
                f.write(contents)

    def __str__(self) -> str:
        return self.path
    
    def __repr__(self) -> str:
        return f"FileObject('{self.path}')"
    
class DirectoryObject:
    """
    Object that stores path properties as strings.
    Available properties: 
        location : TODO
        path     : absolute/path/to/file.ext,
        basename : file.ext,
        listing  : [sub-file?, sub-directory?]
    """
    location: str   # TODO
    path: str
    basename: str
    listing: List[Union[FileObject, DirectoryObject]]
    
    def __init__(self, dir_path: str | Path | DirectoryObject):
        if isinstance(dir_path, str):
            dir_path = Path(dir_path)
        if isinstance(dir_path, Path):
            # path: Path = dir_path.resolve() < BUG 
            # pathlib.Path.resolve resolves symlinks, which is unwanted
            # behaviour, as we are sometimes pointing to symlinks. Normalizing
            # the parent and adding the name part circumvents this.
            path: Path = dir_path.parent.resolve() / dir_path.name

            self.path = str(path)
            self.basename = path.name
            # location: str = #TODO
            # NOTE Does this need to be recursive? 
            self.listing = []
            for p in path.iterdir():
                if p.is_dir():
                    self.listing.append(DirectoryObject(str(p)))
                if p.is_file():
                    self.listing.append(FileObject(str(p)))
        elif isinstance(dir_path, DirectoryObject):
            self.path = dir_path.path
            self.basename = dir_path.basename
            self.listing = deepcopy(dir_path.listing)
            # self.location = dir_path.location    # TODO
        else:
            raise Exception(f"DirectoryObject expects 'str' | 'Path' | 'DirectoryObject', but found '{type(dir_path)}'")

    def resolve(self) -> DirectoryObject:
        return DirectoryObject(Path(self.path).resolve())
    
    def resolve_as_str(self) -> str:
        return str(Path(self.path).resolve())
    
    def create(self) -> None:
        Path(self.path).mkdir(parents = True)

    def __str__(self) -> str:
        return self.path
    
    def __repr__(self) -> str:
        return f"DirectoryObject({self.path})"
    
    
"""
Mapping of Python types to CWL types. CWL supports types that base Python does
not recognize or support, like double and long. FIXME This is a band-aid for now.
"""
PY_CWL_T_MAPPING: dict[Type, List[str]] = {
    NoneType: ["null"],
    Absent: ["null"],
    bool: ["boolean"],
    int: ["int", "long"],
    float: ["float", "double"],
    str: ["string", "file", "directory"],
    FileObject: ["file"], 
    DirectoryObject: ["directory"],
}

"""
Mapping of CWL types to Python types. CWL supports types that base Python does not
recognize or support, like double and long. FIXME This is a band-aid for now.
"""
CWL_PY_T_MAPPING: dict[str, Type] = {
    "null": NoneType,
    "boolean": bool,
    "int": int,
    "long": int,
    "float": float,
    "double": float,
    "string": str,
    "file": FileObject,
    "directory": DirectoryObject,
}


class Value:
    """
    Wrapper for a value and its corresponding Python and CWL datatype.
    """
    value: Any
    type: Type
    cwltype: str
    is_array: bool
    # is_map: bool

    def __init__(self, value: Any, type_t: Type, cwl_type: str) -> None:
        if isinstance(value, Mapping):
            raise TypeError("Value class does not support map types.")
        if type_t not in PY_CWL_T_MAPPING:
            raise ValueError(f"Unsupported Python type: {type_t}")
        if cwl_type not in CWL_PY_T_MAPPING:
            raise ValueError(f"Unsupported CWL type: {cwl_type}")

        self.value = value
        self.type = type_t
        self.cwltype = cwl_type
        self.is_array = isinstance(value, Sequence)


    def get(self, index: int) -> Value | None:
        """
        Retrieve an element at ``index`` from the array.
        Returns ``None`` if ``value`` does not contain an array or if the index
        is out of bounds.
        """
        if not isinstance(self.value, Sequence): 
            return None
        if index >= len(self.value): 
            return None
        return Value(self.value[index], self.type, self.cwltype)
        # return self.value[index]


    # def is_array(self):
    #     return isinstance(self.value, Sequence)
    
    # def is_map(self):
    #     return isinstance(self.value, Mapping)

    def __str__(self) -> str:
        return str(self.value)
    
    def __repr__(self) -> str:
        return f"Value({self.value}, {self.type}, {self.cwltype})"