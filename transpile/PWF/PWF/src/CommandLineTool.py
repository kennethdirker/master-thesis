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


    def compose_argument(
            self,
            arg_dict
            ) -> str:
            cmd = ""

            # Apply prefix to argument
            if "prefix" in arg_dict:
                cmd = arg_dict["prefix"] + " " + cmd
            return cmd

    def compose_commandline(
            self,
            args: list[Tuple[str, dict[str, Any]]]
            # keywords: list[str]
        ) -> list[str]:
        """
        Insert.
        TODO Description
        """
        # TODO Support optional arguments (type: '*?')
        cmds: list[str] = []
        for arg_id, arg_dict in args:
            if "[]" in arg_dict["type"]:
                # Handle array type
                # # Cannot combine 'separate=False' and 'itemSeparator' properties 
                # if ("separate" in arg_dict and 
                #     "separate" is False and
                #     "itemSeparator" in arg_dict):
                #     raise Exception(f"'{arg_id}: 'separate: False' and 'itemSeparator' are exclusive")
                    
                arg_type: str =  arg_dict["type"][:-2]
                separate: bool = True   #default
                delim = None
                if "separate" in arg_dict["separate"]:
                    separate = arg_dict["separate"]

                if "itemSeparator" in arg_dict["itemSeparator"]:
                    delim = arg_dict["itemSeparator"]

                if delim:
                    for item in self.runtime_inputs[arg_id]:
                        cmds
                else:




                # if "itemSeparator" in arg_dict["type"]: # -iA,B,C

                # elif "separate" in arg_dict["type"] and not arg_dict["separate"]:   # -iA -iB -iC
                #     for value in self.runtime_inputs[arg_id]:
                #         cmd += self.
                # elif "separate" in arg_dict["type"] and arg_dict
                # else:   # -i A B C
                #     cmd = arg_dict["itemSeparator"] + " ".join(self.runtime_inputs[arg_id])
            else:
                # Handle non-array type
                to_insert = self.runtime_inputs[arg_id]
                cmd = self.compose_argument(to_insert, arg_dict)


            cmds.append(cmd)
        return cmds


    def run_command_line(
            self,
            args: list[Tuple[str, dict[str, Any]]],
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
        cmd: list[str] = self.compose_commandline(args, keywords)
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