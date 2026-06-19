from __future__ import annotations

import numpy as np
import os
import shutil

from typing import (
    Any,
    List,
    Optional,
    Tuple,
    Type,
    Sequence,
    Mapping,
    MutableMapping,
    Union,
    cast
)

from types import NoneType
from pathlib import Path
from copy import deepcopy

from cwl_utils.parser.cwl_v1_2 import File as CWLFile
from cwl_utils.parser.cwl_v1_2 import Directory as CWLDirectory

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

    TODO contents/size
    TODO location prefixes (file://, http://, https://)
    """
    attrs = [
        "location", "path", "basename", "dirname", "nameroot", "nameext",
        "contents", "size", "writable"
    ]

    location: str
    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: str
    contents: str
    size: int
    writable: bool
    # MAX_SIZE: int = 64000
    
    def __init__(
            self, 
            file_path: str | Path | FileObject | CWLFile | Mapping
        ):
        
        def load(o: Any, attr: str):
            if hasattr(o, attr) and getattr(o, attr) is not None:
                setattr(self, attr, getattr(o, attr))

        self.location = ""
        self.path = ""
        self.basename = ""
        self.dirname = ""
        self.nameroot = ""
        self.nameext = ""
        self.contents = ""
        self.size = 0
        self.writable = False

        if isinstance(file_path, str):
            file_path = Path(file_path)
        
        if isinstance(file_path, Path):
            # path: Path = file_path.resolve() < BUG dont use
            # pathlib.Path.resolve resolves symlinks, which is unwanted
            # behaviour, as we are sometimes pointing to symlinks. Normalizing
            # the parent and adding the name part circumvents this.
            # path: Path = file_path.parent.resolve() / file_path.name
            self.set_path_attributes(file_path)
            self.location = self.path
        elif isinstance(file_path, FileObject | CWLFile):
            load(file_path, "location")
            load(file_path, "path")
            if self.location == "" and self.path != "":
                self.location = self.path
            if self.path == "" and self.location != "":
                self.path = self.location
            self.set_path_attributes(self.path)
            load(file_path, "basename")
            load(file_path, "dirname")
            load(file_path, "nameroot")
            load(file_path, "nameext")
            load(file_path, "contents")
            load(file_path, "size")
            load(file_path, "writable")
        elif isinstance(file_path, MutableMapping):
            if ("location" in file_path and file_path["location"] != "" 
                and ("path" not in file_path or
                    ("path" in file_path and file_path["path"] == ""))):
                file_path["path"] = file_path["location"]
            if "path" in file_path:
                self.set_path_attributes(file_path["path"])
            for k, v in file_path.items():
                if k in self.attrs:
                    setattr(self, k, v)
        else:
            raise Exception(f"FileObject expects 'str' | 'Path' | 'FileObject | cwl_utils.parser.cwl_v1_2.File', but found '{type(file_path)}'")
        

    def set_path_attributes(self, path: str | Path) -> None:
            if isinstance(path, str):
                path = Path(path)
            # path: Path = file_path.resolve() < BUG dont use
            # pathlib.Path.resolve resolves symlinks, which is unwanted
            # behaviour, as we are sometimes pointing to symlinks. Normalizing
            # the parent and adding the name part circumvents this.
            path = path.parent.resolve() / path.name
            self.path = str(path)
            self.basename = path.name
            self.dirname = str(path.parent)
            self.nameroot = path.stem
            self.nameext = path.suffix 


    def resolve(self) -> FileObject:
        return FileObject(Path(self.path).resolve())
    

    def resolve_as_str(self) -> str:
        return str(Path(self.path).resolve())
    

    def exists(self) -> bool:
        return Path(self.path).exists()
    

    def copy(self, target: str | Path) -> None:
        if isinstance(target, str):
            target = Path(target)
        if not isinstance(target, Path):
            raise Exception(f"Expected 'str' or 'Path', but found {type(target)}")
        shutil.copy2(Path(self.path).resolve(), target)
        

    def link(self, target:  str | Path) -> None:
        if isinstance(target, str):
            target = Path(target)
        if not isinstance(target, Path):
            raise Exception(f"Expected 'str' or 'Path', but found {type(target)}")
        os.symlink(Path(self.path).resolve(), target)
    

    def create(self, contents: Optional[str] = None) -> None:
        with open(self.path, "w") as f:
            if contents is not None:
                f.write(contents)
                # if loadContents:  # TODO Needed?
                #     self.contents = contents
            elif hasattr(self, "contents"):
                f.write(self.contents)


    def rebase(self, new_path: str | Path) -> None:
        self.set_path_attributes(new_path)


    def __str__(self) -> str:
        return self.path
    

    def to_dict(self) -> dict:
        return {k: getattr(self, k) 
                for k in self.attrs 
                if hasattr(self, k) and getattr(self, k) is not None}
    

    def __repr__(self) -> str:
        pairs = [f'"{k}":"{getattr(self, k)}"' 
                 for k in self.attrs 
                 if hasattr(self, k) and getattr(self, k) is not None]
        return f"FileObject({', '.join(pairs)})"
    
    
class DirectoryObject:
    """
    Object that stores path properties as strings.
    Available properties: 
        location : TODO
        path     : absolute/path/to/file.ext,
        basename : file.ext,
        listing  : [sub-file?, sub-directory?]
    """
    # TODO handle listings?
    location: str   # TODO
    path: str
    basename: str
    listing: List[Union[FileObject, DirectoryObject]]
    attrs = ["location", "path", "basename", "listing"]
    
    def __init__(
            self,
            dir_path: str | Path | DirectoryObject | CWLDirectory | Mapping):

        def load(o: Any, attr: str):
            if hasattr(o, attr) and getattr(o, attr) is not None:
                setattr(self, attr, getattr(o, attr))

        self.location = ""
        self.path = ""
        self.basename = ""
        self.listing = []
        
        if isinstance(dir_path, str):
            dir_path = Path(dir_path)

        if isinstance(dir_path, Path):
            # path: Path = dir_path.resolve() < BUG 
            # pathlib.Path.resolve resolves symlinks, which is unwanted
            # behaviour, as we are sometimes pointing to symlinks. Normalizing
            # the parent and adding the name part circumvents this.
            path: Path = dir_path.parent.resolve() / dir_path.name
            self.set_path_attributes(path)
            self.location = self.path
            # NOTE Does this need to be recursive? 
            for p in path.iterdir():
                if p.is_dir():
                    self.listing.append(DirectoryObject(str(p)))
                if p.is_file():
                    self.listing.append(FileObject(str(p)))
        elif isinstance(dir_path, DirectoryObject | CWLDirectory):
            load(dir_path, "location")
            load(dir_path, "path")
            if self.location == "" and self.path != "":
                self.location = self.path
            if self.path == "" and self.location != "":
                self.path = self.location
            self.set_path_attributes(self.path)
            load(dir_path, "basename")
            load(dir_path, "listing")
        elif isinstance(dir_path, MutableMapping):
            if ("location" in dir_path and dir_path["location"] != "" 
                and ("path" not in dir_path or 
                    ("path" in dir_path and dir_path["path"] == ""))):
                dir_path["path"] = dir_path["location"]
            if "path" in dir_path:
                self.set_path_attributes(dir_path["path"])
            for k, v in dir_path.items():
                if k in self.attrs:
                    setattr(self, k, v)
        else:
            raise Exception(f"DirectoryObject expects 'str' | 'Path' | 'DirectoryObject', but found '{type(dir_path)}'")
        

    def set_path_attributes(self, path: str | Path) -> None:
            if isinstance(path, str):
                path = Path(path)
            self.path = str(path)
            self.basename = path.name


    def resolve(self) -> DirectoryObject:
        return DirectoryObject(Path(self.path).resolve())
    

    def resolve_as_str(self) -> str:
        return str(Path(self.path).resolve())
    

    def exists(self) -> bool:
        return Path(self.path).exists()
    

    def copy(self, target: str | Path) -> None:
        if isinstance(target, str):
            target = Path(target)
        if not isinstance(target, Path):
            raise Exception(f"Expected 'str' or 'Path', but found {type(target)}")
        shutil.copytree(Path(self.path).resolve(), target, symlinks=False)  #TODO copy symlinks?


    def link(self, target: str | Path) -> None:
            if isinstance(target, str):
                target = Path(target)
            if not isinstance(target, Path):
                raise Exception(f"Expected 'str' or 'Path', but found {type(target)}")
            os.symlink(Path(self.path).resolve(), target, target_is_directory=True)


    def create(self) -> None:
        Path(self.path).mkdir(parents = True)


    def rebase(self, new_path: str | Path) -> None:
        self.set_path_attributes(new_path)


    def __str__(self) -> str:
        return self.path
    

    def __repr__(self) -> str:
        pairs = [f'"{k}":"{getattr(self, k)}"' 
                 for k in self.attrs 
                 if hasattr(self, k) and getattr(self, k) is not None]
        return f"DirectoryObject({', '.join(pairs)})"
    
    
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

    def __init__(
            self,
            value: Any,
            type_t: Type,
            cwl_type: str,
            scattered: bool = False
        ) -> None:
        """
        TODO
        """
        if isinstance(value, Mapping):
            raise TypeError("Value class does not support map types.")
        if type_t not in PY_CWL_T_MAPPING:
            raise ValueError(f"Unsupported Python type: {type_t}")
        if cwl_type not in CWL_PY_T_MAPPING:
            raise ValueError(f"Unsupported CWL type: {cwl_type}")

        self.value = value
        self.type = type_t
        self.cwltype = cwl_type
        self.is_array = scattered | isinstance(value, np.ndarray) | \
                (isinstance(value, Sequence) and not isinstance(value, str))
        self.scattered = scattered

    
    def to_list(self) -> None:
        """
        If this value is scattered, transform the ndarray in ``value`` to a
        (nested) list.
        """
        if self.scattered:
            self.value = self.value.to_list()


    def scatterize(
            self, 
            shape: Tuple[int, ...], 
            idx: Tuple[int, ...]
        ) -> Value:
        """
        Return a scattered version of this ``Value``, replacing ``value`` with
        an empty multi-dimensional array where the previous ``value`` is 
        inserted at ``idx``.
        """
        arr = np.ndarray(shape, self.type)  # < new NOTE
        # arr = np.ndarray(shape, Value)    # < old
        arr[idx] = self.value
        return Value(arr, self.type, self.cwltype, scattered=True)


    def get(
            self, 
            idx: int | Tuple[int, ...]
        ) -> Value | None:
        """
        Retrieve an element at ``index`` from the (multi-dimensional) array.
        Returns ``None`` if ``value`` does not contain an array.
        """
        if not isinstance(self.value, Sequence): 
            return None
        if isinstance(idx, int):
            idx = (idx,)
        v = self.value
        for i in idx[:-1]:
            v = v[i]
        return Value(v[idx[-1]], self.type, self.cwltype)

    
    def set(
            self, 
            idx: int | Tuple[int, ...], 
            value: Any
        ) -> None:
        """
        Set the element at ``index`` from the (multi-dimensional) array to 
        ``value``. Returns ``None`` if ``value`` does not contain an array.
        """
        if isinstance(idx, int):
            idx = (idx,)
        v = self.value
        for i in idx[:-1]:
            v = v[i]
        v[idx[-1]] = value


    def __str__(self) -> str:
        if self.is_array:
            return "[" + ", ".join([str(v) for v in self.value]) + "]"
        return str(self.value)
    

    def __repr__(self) -> str:
        return f"Value({self.value}, {self.type}, {self.cwltype})"