from abc import abstractmethod
from subprocess import run
from .process import BaseProcess
# from .utils import Absent

class BaseCommandLineTool(BaseProcess):

    def __init__(self):
        """ TODO: class description """
        super.__init__()


    # @abstractmethod
    # def positionals(self) -> list:
    #     pass


    # @abstractmethod
    # def optionals(self) -> dict:
    #     pass

    @abstractmethod
    def base_command(self) -> str:
        """
        Must be overridden
        """
        # Example:
        # 
        # 
        # 
        pass


    def run_command_line(self, cmd):
        """
        Wrapper for subprocess.run().

        The wrapper is lazily executed in a dask.delayed task graph.
        """
        # TODO: Add support for:
        #     stdin
        #     stdout
        #     stderr
        # TODO Get runtime input variables and check if present
        run(cmd)
    
    def execute(self) -> None:
        """ Wrapper for tool execution. Can be overwritten to alter execution behaviour. """
        # Get requirements
        # Build command line
        raise NotImplementedError