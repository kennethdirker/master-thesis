from typing import Any
from abc import ABC


class Process(ABC):
    """
    Represents a single executable step in the workflow.
    """

    # TODO figure out what this class will do.

    @abstractmethod
    def _in_script(self):
        """ Script performed on the input data of the process """
        pass


    @abstractmethod
    def _out_script(self):
        """ Script performed on the output data of the process. """
        pass


    @abstractmethod
    # def execute(
    def load(
        self,
        runner,
        input_objs = None,
        output_objs = None,
        image_url: str = None,
    ):
        # How the Process is executed by the runner:
        # Start container if needed
        # Load input
        # Input manipulation with _in_script()
        # Command is executed by Runner
        # Output manipulation with _out_script()
        # Export output? NOTE: What if we use mounted volume?
        # Stop container if needed
        pass


class GroupedProcess():
    """
    Process but with support for grouped steps.
    """

    # TODO figure out what this class will do.

    pass