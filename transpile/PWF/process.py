import yaml

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Union

class BaseProcess(ABC):
    def __init__(self, runtime_inputs_uri: str | None = None):
        """ TODO: class description """
        # Assign metadata attributes
        self.id:    Union[str, None] = None # Unique ID, used internally
        self.label: Union[str, None] = None # Human readable short description
        self.doc:   Union[str, None] = None # Human readable process explaination

        # Assign input/output
        # FIXME: dicts should probably use special classes like CWL does
        self.inputs_dict:  Union[dict] = {} 
        self.outputs_dict: Union[dict] = {}

        # Assign requirements and hints
        # NOTE: Probably not needed for Minimal Viable Product
        self.reqs:  Union[dict, None] = {}
        self.hints: Union[dict, None] = {}
        
        # Load runtime input variables from a YAML into a dictionary
        # NOTE: runtime_inputs_uri argument is only used in the root process!
        self.runtime_inputs: dict = None
        if runtime_inputs_uri:
            self._load_yaml(runtime_inputs_uri)

        # TODO: Prepare Dask?
        # dask_client = ...


    def _load_yaml(yaml_uri: str) -> dict:
        """
        Load a YAML file pointed at by 'yaml_uri' into a dictionary.
        """
        return yaml.safe_load(Path(yaml_uri).read_text())

    @abstractmethod
    def metadata(self):
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
    def execute(self) -> None:
        """ This function must be overridden to implement execution behaviour. """
        # Example:
        # 
        # 
        # 
        pass
    

    def runnable(self, runtime_inputs_dict: dict) -> bool:
        """
        Check whether a process is ready to be executed.

        Returns:
            True if the process can be run, False otherwise.
        """
        # TODO: Currently only checks that all keys are matched. Additions: 
        # - Validate type of inputs
        # - 
        # NOTE: Extra checks probably not needed for Minimal Viable Product.
        for key, value in self.inputs().items():
            if key not in runtime_inputs_dict:
                return False 
        return True