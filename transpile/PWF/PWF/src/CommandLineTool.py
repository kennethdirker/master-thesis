import dask.delayed
# import inspect
# import uuid

from abc import abstractmethod
from subprocess import run
from typing import Any, Optional, Tuple

from .Process import BaseProcess


class BaseCommandLineTool(BaseProcess):

    def __init__(
            self,
            main: bool = False,
            runtime_inputs: Optional[dict] = None
        ):
        """ TODO: class description """
        super().__init__(main=main, runtime_inputs=runtime_inputs)
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

        Can be overwritten by user to assign the following variables:
            - self.id: Unique identity of a process instance.
            - self.label: Human readable short description.
            - self.doc: Human readable process explaination.
            - TODO: Add more fields if needed!
        """
        pass        

    @abstractmethod
    # FIXME: Better function name
    def command_line(self) -> None:
        """
        TODO: Description
        """
        # Example:
        # self.base_command = "ls -l"
        # self.base_command = f"{self.inputs_dict['dynamic_program']}"
        pass


    def run_command_line(
            self,
            cmd_template: str,
            keywords: str
        ):
        """
        Wrapper for subprocess.run().

        The wrapper is lazily executed in a dask.delayed task graph.
        """
        # TODO: Add support for:
        #     stdin
        #     stdout
        #     stderr
        # TODO Get runtime input variables and check if present
        cmd = self.insert_inputs(cmd_template, keywords)
        print(cmd)
        run(cmd)
        # TODO process outputs
        #  


    def insert_inputs(
            self,
            cmd: str,
            keywords: list[str]
        ) -> str:
        """
        TODO Description
        """
        # TODO: Should we check if all keywords are present?
        for keyword in keywords:
            cmd = cmd.replace(f"${keyword}$", self.runtime_inputs[keyword])
        return cmd


    def parse_args(
            self,
            args: list[Tuple[str, dict[str, Any]]]
        ) -> list[str]:
        """
        TODO Description
        """
        parsed_args: list[str] = []
        for id, arg_dict in args:
            parsed_arg: str = ""
            if "prefix" in arg_dict:
                parsed_arg = arg_dict["prefix"] + " "
            parsed_arg += f"${id}$"

            parsed_args.append(parsed_arg)
        return parsed_args


    def execute(self) -> None:
        """ Executes this tool. Can be overwritten to alter execution behaviour. """
        # TODO: Get requirements
        pos_args: Tuple[str, dict[str, Any]] = []
        key_args: Tuple[str, dict[str, Any]] = []
        keywords: list[str] = []
        # Decide whether this argument is positional or not.
        for input_id, input_dict in self.inputs_dict.items():
            if hasattr(input_dict, "position"):
                pos_args.append((input_id, input_dict))
            else:
                key_args.append((input_id, input_dict))
            keywords.append(input_id)

        # Order arguments
        args = sorted(pos_args, key=lambda x: x[1]["position"])
        args += key_args
        
        # Parse arguments
        parsed: list[str] = self.parse_args(args)
        cmd_template: str = " ".join([self.base_command, *parsed])

        # Check if all requirements to run the process are met
        runnable, missing = self.runnable()
        if runnable:
            self.task_graph_ref = dask.delayed(self.run_command_line)(
                cmd_template,
                keywords
            )
            self.task_graph_ref.compute()
        else:
            raise RuntimeError(
                f"{self.id} is missing inputs {missing} and cannot run")