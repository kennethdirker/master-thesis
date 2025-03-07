import dask.delayed
import inspect
import uuid

from abc import abstractmethod
from subprocess import run
from typing import Optional

from .process import BaseProcess


class BaseCommandLineTool(BaseProcess):

    def __init__(
            self,
            main: bool = False,
            runtime_inputs: Optional[dict] = None
        ):
        """ TODO: class description """
        super.__init__(main=main, runtime_inputs=runtime_inputs)
        self.metadata()
        self.inputs()
        self.outputs()
        self.requirements()
        self.command_line()
        if main:
            self.execute()


    def metadata(self):
        """
        Assign metadata to the process. Assigns a process identity
        consisting of '{path/to/process}:{uuid}'.
        # FIXME User doesn't see self.id, ommit it from them?

        Can be overwritten by user to assign the following variables:
            - self.id: Unique identity of a process instance.
            - self.label: Human readable short description.
            - self.doc: Human readable process explaination.
            - TODO: Add more fields if needed!
        """
        # FIXME User doesn't see self.id, ommit it from them?
        self.id: str = inspect.getfile(type(self)) + uuid.uuid()
        

    @abstractmethod
    def command_line(self) -> None:
        """
        """
        # Example:
        # self.base_command = "ls -l"
        # self.base_command = f"{self.inputs_dict['dynamic_program']}"
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
        # TODO process outputs
        #  

    def execute(self) -> None:
        """ Wrapper for tool execution. Can be overwritten to alter execution behaviour. """
        # TODO: Get requirements
        pos_args = []
        key_args = []
        # Decide whether this argument is positional or not.
        for arg_id, arg_dict in self.inputs_dict.items():
            if hasattr(arg_dict, "position"):
                pos_args.append((arg_id, arg_dict))
            else:
                key_args.append((arg_id, key_args))
        
        # Process positional arguments
        # TODO

        # Process keyword arguments
        # TODO

        cmd: str = self.base_command
        cmd += " ".join(pos_args)
        cmd += " ".join(key_args)

        # Check if all requirements to run the process are met
        runnable, missing = self.runnable()
        if runnable:
            self.task_graph_ref = dask.delayed(self.run_command_line)(cmd)
            self.task_graph_ref.compute()
        else:
            raise RuntimeError(
                f"{self.id} is missing inputs {missing} and cannot run")