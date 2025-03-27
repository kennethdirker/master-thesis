import dask.delayed
# import inspect
# import uuid
import os

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
        self._process_inputs()
        self.outputs()
        self.requirements()
        self.command_line()
        self.create_graph()
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

        
    def load_runtime_arg(self, arg_id):
        # FIXME: Shouldn't be needed if YAML is loaded as string
        return str(self.runtime_inputs[arg_id])


    def load_runtime_arg_array(
            self,
            arg_id: str,
            arg_type: str
        ) -> list[str]:
        """
        TODO desc
        """
        args: list[str] = []

        if "null" in arg_type:
            return []
        if "bool" in arg_type:
            # NOTE: At the moment not sure how this should look like/ be implemented.
            # FIXME yaml safe_load converts yaml strings to likely Python types.
            # This is a problem here, as 'True' and 'true' both convert to the
            # Python bool type with value True.
            raise NotImplementedError()
        else:
            args = [str(item) for item in self.runtime_inputs[arg_id]]
        return args


    def compose_array_arg(
            self,
            arg_id: str,
            arg_dict: dict[str, Any]
        ) -> list[str]:
        """
        Compose a single command line array argument.

        TODO Support for optional arguments 
        TODO Support for bool type
        """
        args: list[str] = []
        arg_type: str = arg_dict["type"]
        prefix: str = ""
        separate: bool = True
        itemSeparator: str = None

        # Load properties
        arg_type = "".join(c for c in arg_type if c not in "[]?")
        if "prefix" in arg_dict:
            prefix = arg_dict["prefix"]
        if "itemSeparator" in arg_dict:
            itemSeparator = arg_dict["itemSeparator"]
        if "separate" in arg_dict:
            separate = arg_dict["separate"]
        
        # Convert array items to strings
        items: list[str] = []
        items = self.load_runtime_arg_array(arg_id, arg_type)

        if "null" in arg_type:
            return []
        elif "bool" in arg_type:
            # NOTE: At the moment not sure how this should be implemented.
            # FIXME yaml safe_load converts yaml strings to likely Python types.
            # This is a problem here, as 'True' and 'true' both convert to the
            # Python bool type with value True.
            raise NotImplementedError()
        else:
            if itemSeparator:
                items = itemSeparator.join(items)
                if prefix and separate:         # -i= A,B,C
                    args.append(prefix)
                    args.append(items)
                elif prefix and not separate:   # -i=A,B,C
                    args.append(prefix + items)
                else:                           # A,B,C
                    args.append(items)
            else:
                if prefix and separate:         # -i= A B C
                    args.append(prefix)
                    args.extend(items)
                if prefix and not separate:     # -i=A -i=B -i=C
                    for item in items:
                        args.append(prefix + item)
                else:                           # A B C
                    args = items
        return args



    def compose_arg(
            self,
            arg_id: str,
            arg_dict: dict[str, Any]
        ) -> list[str]:
        """
        Compose a single command line argument.
        
        TODO Support for optional arguments 
        TODO Support for bool type
        """
        args: list[str] = []
        prefix: str = ""
        separate: bool = True
        arg_type: str = arg_dict["type"]

        if "prefix" in arg_dict:
            prefix = arg_dict["prefix"]
        if "separate" in arg_dict:
            separate = arg_dict["separate"]

        if "null" in arg_type:
            return []
        elif "bool" in arg_type:
            # FIXME yaml safe_load converts yaml strings to likely Python types.
            # This is a problem here, as 'True' and 'true' both convert to the
            # Python bool type with value True.
            
            # if prefix and self.runtime_inputs[arg_id]:
            #     args.append(prefix)
            # else:
            #     args.append(str(self.runtime_inputs))
            raise NotImplementedError()
        else:
            if separate:
                if prefix:
                    args.append(prefix)
                args.append(self.load_runtime_arg(arg_id))
            else:
                args.append(prefix + self.load_runtime_arg(arg_id))


        return args


    def compose_command(
            self,
            args: list[Tuple[str, dict[str, Any]]]
        ) -> list[str]:
        cmds: list[str] = []
        for arg_id, arg_dict  in args:
            if "[]" in arg_dict["type"]:
                # Compose array argument
                cmds.extend(self.compose_array_arg(arg_id, arg_dict))
            else:
                # Compose single argument
                cmds.extend(self.compose_arg(arg_id, arg_dict))
        return cmds

    def cmd_wrapper(
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
        cmd: list[str] = self.compose_command(args)
        if hasattr(self, "base_command"):
            if isinstance(self.base_command, list):
                cmd = [*self.base_command] + cmd
            else:
                cmd = [self.base_command] + cmd
        print("Current working directory:", os.getcwd())
        # print(cmd)
        print("Executing:", " ".join(cmd))
        print()
        run(cmd)
        # TODO process outputs
        #  


    def create_graph(self) -> None:
        """
        Build a Dask Delayed object to execute the wrapped tool with.
        """
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
        # runnable, missing = self.runnable()
        # if runnable:
        self.task_graph_ref = dask.delayed(self.cmd_wrapper)(args)
        # else:
            # raise RuntimeError(
                # f"{self.id} is missing inputs {missing} and cannot run")