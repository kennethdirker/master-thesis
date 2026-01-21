from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, List, Optional, Tuple, Type, Sequence, Mapping, Union
from types import NoneType
from js2py.base import JsObjectWrapper

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
    
    path: str = ""
    basename: str = ""
    dirname: str = ""
    nameroot: str = ""
    nameext: str = ""
    
    def __init__(self, file_path: str | FileObject):
        if isinstance(file_path, str):
            path: Path = Path(file_path).resolve()
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
        else:
            raise Exception(f"FileObject expects 'str' or 'FileObject', but found '{type(file_path)}'")

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
    listing: List[Union[FileObject, 'DirectoryObject']]
    
    def __init__(self, file_path: str):
        path: Path = Path(file_path).resolve()
        self.path = str(path)
        self.basename = path.name
        # location: str = #TODO

        self.listing = []
        for p in path.iterdir():
            if p.is_dir():
                self.listing.append(DirectoryObject(str(p)))
            if p.is_file():
                self.listing.append(FileObject(str(p)))

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


    # def is_array(self):
    #     return isinstance(self.value, Sequence)
    
    # def is_map(self):
    #     return isinstance(self.value, Mapping)

    def __str__(self) -> str:
        return str(self.value)
    
    def __repr__(self) -> str:
        return f"Value({self.value}, {self.type}, {self.cwltype})"
    


class ScatterMethod(Enum):
    DOTPRODUCT = "dotproduct"
    NESTED_CROSSPRODUCT = "nested_crossproduct"
    FLAT_CROSSPRODUCT = "flat_crossproduct"

class Scatter:
    gather: Gather
    input_ids: List[str]   # Input IDs linked to the scatter
    step_uid: str
    method: ScatterMethod

    def __init__(
            self,
            input_ids: List[str],
            step_uid: str,
            method: ScatterMethod = ScatterMethod.DOTPRODUCT
        ):
        if not isinstance(input_ids, List):
            raise Exception(f"Expected list, but found {type(input_ids)}")
        if len(input_ids) == 0:
            raise Exception("List of associated inputs cant be empty")
        if not isinstance(input_ids[0], str):
            raise Exception(f"Expected list of 'str', but found list of {type(input_ids[0])}")
        if not isinstance(step_uid, str):
            raise Exception(f"Expected 'str', but found '{type(step_uid)}'")
        if not isinstance(method, ScatterMethod):
            raise Exception(f"Expected 'ScatterMethod' enum, but found '{type(method)}'")
        
        self.input_ids = input_ids
        self.step_uid = step_uid
        self.method = method


class Gather:
    scatter: Scatter
    step_uid: str
    method: ScatterMethod
    gathered: int
    total_items: int
    active: bool

    def __init__(
            self,
            scatter: Scatter
        ):
        self.method = scatter.method


    def done(self):
        return self.gathered == self.total_items
    

    def retrieve(self) -> dict[str, Value]:
        ...


    def add(self, items, loc) -> bool:

        return self.done()


def get_scatter_gather(
        method: Optional[str] = None
    ) -> Tuple[Scatter, Gather]:
    
    scatter = Scatter()
    gather = Gather()

    # Link scatter and gather
    # scatter.gather = gather
    # gather.scatter = scatter
    return scatter, gather



JsArrayTypes = ('Array', 'Int8Array', 'Uint8Array', 'Uint8ClampedArray', 
                'Int16Array', 'Uint16Array', 'Int32Array', 'Uint32Array',
                'Float32Array', 'Float64Array', 'Arguments')

def JsObjectWrapper_is_list(obj: JsObjectWrapper):
    if not isinstance(obj, JsObjectWrapper):
        raise Exception(f"Expected JsObjectWrapper, but found '{type(obj)}'")
    return obj._obj.Class in JsArrayTypes # type: ignore
