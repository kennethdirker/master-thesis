import copy
import dask.delayed
import inspect
import sys
import uuid
import yaml

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Union

class BaseProcess(ABC):
    def __init__(
            self,
            main: bool = False,
            runtime_inputs: Optional[dict] = None
        ):
        """ 
        TODO: class description 
        BaseProcess __init__ should be run before subclass __init__.

        Arguments:
            runtime_inputs: Must only be used if this process is not the
            root process.
        """
        # A process that is called from the command line is root. Processes
        # that are instantiated from other another process (sub-processes)
        # are not root.
        self.is_root: bool = main

        # Assign metadata attributes. Override in self.metadata().
        # Unique identity of a process instance. The id looks as follows:
        #   "{path/to/process/file}:{uuid}"  
        self._id = inspect.getfile(type(self)) + str(uuid.uuid4())
        self.label: Union[str, None] = None # Human readable process name.
        self.doc:   Union[str, None] = None # Human readable process explaination.
        
        # Assign input/output dictionary attributes.
        # FIXME: dicts should probably use special classes like CWL does.
        self.inputs_dict:  Union[dict] = {} # Override in self.inputs()
        self.outputs_dict: Union[dict] = {} # Override in self.outputs()

        # Assign requirements and hints.
        # Override in self.requirements() and self.hints().
        # NOTE: Probably not needed for Minimal Viable Product.
        self.reqs:  Union[dict, None] = {}
        self.hints: Union[dict, None] = {}
        
        # Assign a dictionary with runtime input variables to the process.
        # Only the root process must load the input YAML from a file. Non-root
        # processes get a reference to the dictionary loaded in the main process.
        # NOTE: ...Might be susceptible to bugs...
        if main:
            # The YAML file uri comes from the first command line argument
            self.runtime_inputs = self._load_yaml(sys.argv[1])
        else:
            if runtime_inputs is None:
                raise Exception(f"Subprocess {type(self)}({self._id}) is not initialized as root process, but isn't given runtime inputs!")
            self.runtime_inputs = runtime_inputs
            
        print("Inputs loaded into self.runtime_inputs:")
        for k, v in self.runtime_inputs.items():
            print("\t- ", k, ":", v)
        print()

        # TODO: Prepare Dask?
        # dask_client = ...
        self.task_graph_ref: Union[dask.Delayed, None] = None



    def _load_yaml(self, yaml_uri: str) -> dict:
        """
        Load a YAML file pointed at by 'yaml_uri' into a dictionary.
        """
        # NOTE: BaseLoader is used to force the YAML reader to only create
        # string objects, instead of interpreting and converting objects to
        # Python objects. This is needed for cases like booleans, where
        # X:true is converted to X:bool(True), which are not identical. 
        with open(Path(yaml_uri), "r") as f:
            y = yaml.load(f, Loader=yaml.BaseLoader)
            if not isinstance(y, dict):
                raise Exception(f"Loaded YAML should be a dict, but has type {type(y)}")
            return y

    @abstractmethod
    def metadata(self) -> None:
        """
        Must be overwritten to assign process metadata attributes.
        """
        pass


    # @abstractmethod
    def inputs(self) -> dict[str, Any] :
        """ 
        This function can be overridden to define input job order field 
        requirements. These job order requirements are used to test whether a 
        tool is ready to be executed.
        """
        # Example:
        # 
        # 
        # 
        # pass
        return {}
    
    
    # @abstractmethod
    def outputs(self) -> dict[str, Any]:
        """
        This function can be overridden to define Process outputs.
        """
        # Example:
        # 
        # 
        # 
        # pass
        return {}


    # @abstractmethod
    def requirements(self) -> dict:
        """
        This function can be overridden to indicate execution requirements.
        """
        # Example:
        # 
        # 
        # 
        # pass
        return {}


    @abstractmethod
    def create_graph(self) -> None:
        """ 
        This function must be overridden to implement building the Dask Task
        Tree. The function must assign the final tree node to 
        'self.task_tree_ref', which is executed by 'self.execute()'.
        """
        # Example:
        # 
        # 
        # 
        pass
    

    def execute(self):
        self.task_graph_ref.compute()


    def __call__(self, runtime_dict: dict) -> bool:
        return self.execute(runtime_dict)


    def runnable(self) -> bool:
    # def runnable(self, runtime_inputs_dict: dict) -> bool:
        """
        Check whether a process is ready to be executed.

        Returns:
            True if the process can be run, False otherwise.
            Additionally, a list of missing inputs is returned.
        """
        # TODO: Currently only checks that all keys are matched. Additions: 
        # - Validate type of inputs.
        # - 
        # # NOTE: Extra checks probably not needed for Minimal Viable Product.
        # FIXME: move to Workflow.runnable
        # if self.task_graph_ref is None:
        #     return False, []
            
        green_light = True
        missing_inputs: list[str] = []
        for key, value in self.inputs_dict.items():
            if key not in self.runtime_inputs:
                # if key not in runtime_inputs_dict:
                green_light = False
                missing_inputs.append(key)
        return green_light, missing_inputs