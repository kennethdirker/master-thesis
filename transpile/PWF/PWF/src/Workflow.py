import importlib
import inspect
import sys

from abc import abstractmethod
from pathlib import Path
from typing import Optional, Tuple, Union

from .Process import BaseProcess

class BaseWorkflow(BaseProcess):
    def __init__(
            self,
            main: bool = False,
            runtime_inputs: Optional[dict] = None
        ):
        """ TODO: class description """
        # Must be overridden by self.steps().
        self.steps_dict: dict[str, dict[str, str]] = {}

        super().__init__(main=main, runtime_inputs=runtime_inputs)
        self.metadata()
        self.inputs()
        self._process_inputs()
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
        step_register: dict[str, Tuple[dict, BaseProcess]] = {}

        # Create processes for each step
        print("Loading steps...")
        for step_id, step_obj in self.steps_dict.items():
            # print(step_id, step_obj)
            step_process = self._load_single_class_from_uri(step_obj["run"])
            step_register[step_id] = (step_obj, step_process)

        # Get process dependencies
        for step_id, [step_obj, process] in step_register.items():
            print("\n", step_id, step_obj, process._id, "\n")
            # Check inputs
            for input_id, input_dict in step_obj["in"].items():
                if "source" in input_dict and "/" in input_dict["source"]:
                    # The step depends on the output of another step
                    o = step_register[input_dict["source"].split("/")[0]]
                    print(o)
                    self.parents.append(o[1]._id)
        print(self.parents)

        raise NotImplementedError
        # self.task_graph_ref
    

    def _load_single_class_from_uri(self, uri: str) -> BaseProcess:
        """
        Dynamic Process loading from file. Raises an exception if no valid 
        process class can be found in the file.
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
                continue
            # Check if the class is imported from the actual file uri, and not
            # from an import statement present in the file.
            if not path.stem in str(obj):
                continue
            # Check if the class inherits from BaseProcess, but is not of type
            # BaseProcess. This check is not necessarily needed, but better 
            # be save then sorry.
            if not issubclass(obj, BaseProcess) and not isinstance(obj, BaseProcess):
                continue

            # Instantiate and return the class
            # NOTE: more checks needed?
            print(f"\tFound process at {uri}")
            return obj(main=False, runtime_inputs=self.runtime_inputs)
        raise Exception(f"{uri} does not contain a BaseProcess subclass")

