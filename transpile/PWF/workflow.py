# import importlib
# import inspect
# import sys

from abc import abstractmethod
# from pathlib import Path
from typing import Union

from .process import BaseProcess

class BaseWorkflow(BaseProcess):
    def __init__(self):
        """ TODO: class description """
        super.__init__()
        self.steps_dict: Union[dict] = {}

    @abstractmethod
    def steps(self):
        """ Defines the sub-processes of this workflow"""
        # Example:
        # 
        # 
        # 
        pass

    def execute(self) -> None:
        """ Execute the workflow. Can be overwritten to alter execution behaviour. """

        raise NotImplementedError
    

    def _load_single_class_from_file(self, file_uri: str) -> BaseProcess:
        """
        
        """
        # NOTE: Dynamic class loading from file (hint):
        # https://python-forum.io/thread-7923.html
        # path = Path(file_uri)
        # if not path.is_file():
        #     raise FileNotFoundError(f"{file_uri} is not a file")
        # sys.path.append(str(path.parent))
        # potential_module = importlib.import_module(path.stem)

        # for class_attr in dir(potential_module):
        #     if inspect.isclass(getattr(potential_module, class_attr)):
        #         return getattr(potential_module, class_attr)
        raise NotImplementedError