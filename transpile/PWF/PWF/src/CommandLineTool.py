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

    def _get_arg_string_old(
        self, 
        arg_id: str, 
        arg_dict: dict[str, Any], 
        is_array: bool) -> str:
        arg_type: str = arg_dict["type"]
        # TODO what to do with arrays?
        if "string" in arg_type:
            return str(self.runtime_inputs[arg_id])
        if "int" in arg_type:
            return str(self.runtime_inputs[arg_id])
        if "float" in arg_type:
            return str(self.runtime_inputs[arg_id])
        if "bool" in arg_type:
            if "prefix" in arg_dict and self.runtime_inputs[arg_id]:
                return arg_dict["prefix"]
            return ""
        if "file" in arg_type:
            return str(self.runtime_inputs[arg_id])
        if "directory" in arg_type:
            return str(self.runtime_inputs[arg_id])
        if "null" in arg_type:
            return ""
        
    def load_runtime_arg(self, arg_id):

    def _compose_command_old(
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
            # NOTE: At the moment not sure how this should be implemented.
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

    def run_command(
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
        """
        NOTE: Only execute with Dask if main?
        Executes this tool. Can be overwritten to alter execution behaviour. 
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
        runnable, missing = self.runnable()
        if runnable:
            # NOTE: Only execute with Dask if main?
            self.task_graph_ref = dask.delayed(self.run_command)(args)
            self.task_graph_ref.compute()
        else:
            raise RuntimeError(
                f"{self.id} is missing inputs {missing} and cannot run")