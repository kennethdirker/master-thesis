from abc import abstractmethod
from subprocess import run
from .process import BaseProcess

class BaseCommandLineTool(BaseProcess):

    @abstractmethod
    def positionals(self) -> list:
        pass


    @abstractmethod
    def optionals(self) -> dict:
        pass

    @abstractmethod
    def base_command(self):
        """ Defines the shell command that executes the wrapped tool. """
        # Example:
        # 
        # 
        # 
        pass

    
    def execute(self, *args, **inputs) -> None:
        """ Wrapper for tool execution. Can be overwritten to alter execution behaviour. """
        
        raise NotImplementedError