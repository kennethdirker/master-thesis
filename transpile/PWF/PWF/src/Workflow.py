import importlib
import inspect
import sys

from abc import abstractmethod
from pathlib import Path
from typing import Optional, Union

from .Process import BaseProcess

class BaseWorkflow(BaseProcess):
    def __init__(
            self,
            main: bool = False,
            runtime_inputs: Optional[dict] = None
        ):
        """ TODO: class description """
        # Must be overridden by self.steps().
        self.steps_dict: Union[dict] = {}

        super().__init__(main=main, runtime_inputs=runtime_inputs)
        self.metadata()
        self.inputs()
        self.outputs()
        self.requirements()
        self.steps()
        self.create_graph()
        if main:
            self.execute()

    @abstractmethod
    def steps(self):
        """ Defines the sub-processes of this workflow"""
        # Example:
        # 
        # 
        # 
        pass

    def create_graph(self) -> None:
        """ 
        Create a Dask task graph with the sub-processes of this workflow. 
        Can be overwritten to alter execution behaviour. 
        """

        raise NotImplementedError
    

    def _load_single_class_from_uri(self, uri: str) -> BaseProcess:
        """
        Dynamic class loading from file. Raises an exception if no valid class
        can be found in the file.
        NOTE: Loading a class from a file that contains multiple subclasses of
        BaseProcess causes undefined behaviour.

        Returns:
            Instantiated subclass of BaseProcess. 
        """
        # Taken from:
        # https://stackoverflow.com/questions/66833453/loading-a-class-of-unknown-name-in-a-dynamic-location
        path = Path(uri)
        if not path.is_file():
            raise FileNotFoundError(f"{uri} is not a file")
        
        # Add path of file to PYTHONPATH, so Python can import from it.
        sys.path.append(str(path.parent))
        potential_module = importlib.import_module(path.stem)

        # Test all attributes in the file for being a class
        for potential_class in dir(potential_module):
            obj = getattr(potential_module, potential_class)
            # Check if mysterious class is indeed a class
            if not inspect.isclass(obj):
                break
            # Check if the class is imported from the actual file uri, and not
            # from an import statement present in the file.
            if not path.stem in str(obj):
                break
            # Check if the class inherits from BaseProcess, but is not of type
            # BaseProcess. This check is not necessarily needed, but better 
            # be save then sorry.
            if not issubclass(obj, BaseProcess) and not isinstance(obj, BaseProcess):
                break

            # Instantiate and return the class
            # NOTE: more checks needed?
            return obj(main=False, runtime_inputs=self.runtime_inputs)
        raise Exception(f"{uri} does not contain a BaseProcess subclass")

