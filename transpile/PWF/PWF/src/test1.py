import importlib
import inspect
import sys

from abc import abstractmethod
from pathlib import Path
from typing import Optional, Union
from test3 import BaseProcess


# NOTE: Dynamic class loading from file (hint):
# https://python-forum.io/thread-7923.html
file_uri = "test2.py"
path = Path(file_uri)
if not path.is_file():
    raise FileNotFoundError(f"{file_uri} is not a file")
sys.path.append(str(path.parent))
potential_module = importlib.import_module(path.stem)

for potential_attr in dir(potential_module):
    attr = getattr(potential_module, potential_attr)
    if inspect.isclass(attr) and path.stem in str(attr):
        # return getattr(potential_module, attr)
        if issubclass(attr, BaseProcess):
            print(attr)
            c = attr()
            print(c, c.id)

# raise NotImplementedError

