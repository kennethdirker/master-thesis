import importlib
import inspect
import sys

from abc import abstractmethod
from pathlib import Path
from typing import Optional, Tuple, Union

from .Process import BaseProcess
from .utils import Graph

class BaseWorkflow(BaseProcess):
    def __init__(
            self,
            main: bool = False,
            runtime_inputs: Optional[dict] = None
        ):
        """
        TODO: class description 
        """
        self._dep_graph = Graph()

        # Must be overridden by self.steps().
        self.steps_dict: dict[str, dict[str, str]] = {}

        # Digest workflow file
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
        """
        Defines the sub-processes of this workflow. Overwriting this function
        is mandatory for workflows 
        """
        # Example:
        # self.steps_dict = {
        #     "download_images": {
        #         "in": {
        #             "url_list": {
        #                 "source": "url_list"
        #             }
        #         },
        #         "out": "output",
        #         "run": "../steps/DownloadImages.py",
        #         "label": "download_images"
        #     },
        #     "imageplotter": {
        #         "in": {
        #             "input_fits": {
        #                 "source": "download_images/output"
        #             },
        #             "output_image": {
        #                 "default": "before_noise_remover.png"
        #             }

        #         },
        #         "out": "output",
        #         "run": "../steps/ImagePlotter.py",
        #         "label": "imageplotter"
        #     }
        # }
        pass

    
    def create_graph(self) -> None:
        """ 
        Create a Dask task graph with the sub-processes of this workflow. 
        Can be overwritten to alter execution behaviour. 
        """
        step_register: dict[str, Tuple[dict, BaseProcess]] = {}

        # Create a Process for each step and cache in step register
        print("Loading steps...")
        for step_id, step_dict in self.steps_dict.items():
            # print(step_id, step_dict)
            step_process = self._load_single_class_from_uri(step_dict["run"])
            step_register[step_id] = (step_dict, step_process)

        # Get process dependencies
        for step_id, [step_dict, step_process] in step_register.items():
            print("\n", step_id, step_dict, step_process._id, "\n")

            # Check inputs
            for step_in_id, step_in_dict in step_dict["in"].items():
                if "source" in step_in_dict and "/" in step_in_dict["source"]:
                    # This step depends on the output of another step
                    parent_step_id, parent_output_id = step_in_dict["source"].split("/")
                    o = step_register[parent_step_id]
                    print(o)
                    self.parents.append(o[1]._id)
        print("\n", self.parents, "\n")

        # Construct dependency graph
        # TODO

        # Optimize dependency graph
        # TODO

        # Construct task_graph
        # TODO

        # self.task_graph_ref
        raise NotImplementedError
    

    def _load_single_class_from_uri(self, uri: str) -> BaseProcess:
        """
        Dynamic Process loading from file. Raises an exception if no valid 
        BaseProcess sub-class can be found in the file.
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

