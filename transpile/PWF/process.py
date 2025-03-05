from abc import ABC, abstractmethod
from typing import Any, Union, Callable

class BaseProcess(ABC):
    @abstractmethod
    def inputs(self) -> dict[str, Any] :
        """ This function must be overwritten to define input requirements. """
        # Example:
        # 
        # 
        # 
        pass


    @abstractmethod
    def outputs(self) -> dict[str, Any]:
        """ This function must be overwritten to define Process outputs """
        # Example:
        # 
        # 
        # 
        pass


    def execute(self) -> None:
        """ This function must be overloaded to implement execution behaviour. """
        # Example:
        # 
        # 
        # 
        pass