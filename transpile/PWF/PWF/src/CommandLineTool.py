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


    # def compose_argument(
    #         self,
    #         arg_dict
    #         ) -> str:
    #         cmd = ""

    #         # Apply prefix to argument
    #         if "prefix" in arg_dict:
    #             cmd = arg_dict["prefix"] + " " + cmd
    #         return cmd

    def get_arg(self, arg_id, arg_dict) -> str:
        arg_type: str = arg_dict["type"]
        # TODO what to do with arrays?
        if "string" in arg_type:
            return self.runtime_inputs[arg_id]
        if "int" in arg_type:
            return int(self.runtime_inputs[arg_id])
        if "float" in arg_type:
            return int(self.runtime_inputs[arg_id])
        if "bool" in arg_type:
            return arg_dict["prefix"] if self.runtime_inputs[arg_id] else ""
        if "file" in arg_type:
            return self.runtime_inputs[arg_id]
        if "directory" in arg_type:
            return self.runtime_inputs[arg_id]
        if "null" in arg_type:
            return ""

    def compose_commandline(
            self,
            args: list[Tuple[str, dict[str, Any]]]
        ) -> list[str]:
        """
        Insert.
        TODO Description
        """
        # TODO Support optional arguments (type: '*?')
        cmds: list[str] = []
        for arg_id, arg_dict in args:
            prefix: str = ""
            separate: bool = True   #default
                
            if "prefix" in arg_dict:
                prefix = arg_dict["prefix"]

            if "separate" in arg_dict["separate"]:
                separate = arg_dict["separate"]
            
            # NOTE: Do we need to cast every array item from self.runtime_inputs to string?
            if "[]" in arg_dict["type"]:
                # Handle array type                    
                itemSeparator: str = None

                if "itemSeparator" in arg_dict["itemSeparator"]:
                    itemSeparator = arg_dict["itemSeparator"]

                if separate:
                    if prefix:
                        cmds.append(prefix)

                    if itemSeparator:   # -i= A,B,C
                        cmds.append(itemSeparator.join(self.runtime_inputs[arg_id]))
                    else:               # -i= A B C
                        for item in self.runtime_inputs[arg_id]:
                            cmds.append(item)
                else:
                    if itemSeparator:   # -i=A,B,C
                        cmds.append(prefix + itemSeparator.join(self.runtime_inputs[arg_id]))
                    else:               # -iA -iB -iC
                        for item in self.runtime_inputs[arg_id]:
                            cmds.append(prefix + item)
            else:
                # Handle non-array type
                if separate:    # -i= A
                    if prefix:
                        cmds.append(prefix)
                    cmds.append
                else:           # -i=A
                    cmds.append(prefix + self.runtime_inputs[arg_id])
        return cmds


    def run_command_line(
            self,
            args: list[Tuple[str, dict[str, Any]]]
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
        cmd: list[str] = self.compose_commandline(args)
        print(cmd)
        run(cmd)
        # TODO process outputs
        #  


    # def parse_args(
    #         self,
    #         args: list[Tuple[str, dict[str, Any]]]
    #     ) -> list[Tuple[str, str, dict[str, Any]]]:
    #     """
    #     NOTE Because arrays introduce multiple ways of parsing, parsing
    #     be done at runtime!
    #     TODO Description
    #     Returns:
    #         [(arg_id, arg_cmd, arg_dict), ...]
    #     """
    #     parsed_args: list[Tuple[str, str, dict[str, Any]]] = []
    #     for id, arg_dict in args:
    #         parsed_arg: str = ""
    #         # Check argument if needed
    #         # if "prefix" in arg_dict:
    #             # parsed_arg = arg_dict["prefix"] + " "

    #         # Add argument replacement token
    #         parsed_arg += f"${id}$"

    #         parsed_args.append((id, parsed_arg, arg_dict))
    #     return parsed_args


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
        args: Tuple[str, dict] = sorted(pos_args, key=lambda x: x[1]["position"])
        args += key_args
        
        # Parse arguments
        # parsed: list[str] = self.parse_args(args)
        # cmd_template: str = " ".join([self.base_command, *parsed])

        # Check if all requirements to run the process are met
        runnable, missing = self.runnable()
        if runnable:
            self.task_graph_ref = dask.delayed(self.run_command_line)(
                args,
                keywords
            )
            self.task_graph_ref.compute()
        else:
            raise RuntimeError(
                f"{self.id} is missing inputs {missing} and cannot run")