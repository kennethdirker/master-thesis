import dask.delayed
import glob
import os
import subprocess

from abc import abstractmethod
from contextlib import chdir
from pathlib import Path
# from subprocess import run
from typing import Any, Optional, Tuple

from .process import BaseProcess

class BaseCommandLineTool(BaseProcess):

    def __init__(
            self,
            main: bool = False,
            runtime_context: Optional[dict] = None,
            loading_context: Optional[dict[str, str]] = None,
            parent_id: Optional[str] = None,
            step_id: Optional[str] = None
        ):
        """ TODO: class description """
        super().__init__(
            main = main,
            runtime_context = runtime_context,
            loading_context = loading_context,
            parent_process_id = parent_id,
            step_id = step_id
        )

        # # Maps input_id to its source id, which is used as key in runtime_context
        # NOTE moved to process for now
        # self.input_to_source: dict[str, str] =  {}  # {input_id, global_source_id}

        # Digest CommandlineTool file
        self.set_metadata()
        self.set_inputs()
        # self._process_inputs()
        self.set_outputs()
        self.set_requirements()
        self.set_base_command()
        
        if main:
            self.register_input_sources()
            self.create_task_graph()
            self.execute()


    def set_metadata(self):
        """
        Assign metadata to the process.

        Can be overwritten by user to assign the following variables:
            - self.label: Human readable short description.
            - self.doc: Human readable process explaination.
            - TODO: Add more fields if needed!
        """
        pass        

    @abstractmethod
    # FIXME: Better function name
    def set_base_command(self) -> None:
        """
        TODO: Description
        """
        # Example:
        # self.base_command = "ls -l"
        # self.base_command = ["ls", "-l"]
        pass


    def register_input_sources(self) -> None:
        """
        Link local process inputs IDs to global input IDs.
        NOTE: Only executed if CommandLineTool is called as main.
        """
        if not self.is_main:
            raise Exception("Not called from main process")
        
        for input_id in self.inputs:
            self.input_to_source[input_id] = self.global_id(input_id)


    def load_runtime_arg_array(
            self,
            input_id: str,
            input_type: str
        ) -> list[str]:
        """
        TODO desc
        """
        args: list[str] = []

        if "null" in input_type:
            return []
        if "bool" in input_type:
            # NOTE: At the moment not sure how this should look like/ be implemented.
            # FIXME yaml safe_load converts yaml strings to likely Python types.
            # This is a problem here, as 'True' and 'true' both convert to the
            # Python bool type with value True.
            raise NotImplementedError()
        else:
            global_source_id: str = self.input_to_source[input_id]
            args = [str(item) for item in self.runtime_context[global_source_id]]
        return args


    def compose_array_arg(
            self,
            input_id: str,
            input_dict: dict[str, Any]
        ) -> list[str]:
        """
        Compose a single command-line array argument.

        TODO desc
        TODO Support for optional arguments 
        TODO Support for bool type
        """
        array_arg: list[str] = []
        input_type: str = input_dict["type"]

        # Set default properties
        prefix: str = ""
        separate: bool = True
        itemSeparator: str = None

        # Load properties
        input_type = "".join(c for c in input_type if c not in "[]?")   # Filter '['/']'/'?' from type
        if "prefix" in input_dict:
            prefix = input_dict["prefix"]
        if "itemSeparator" in input_dict:
            itemSeparator = input_dict["itemSeparator"]
        if "separate" in input_dict:
            separate = input_dict["separate"]

        # Convert array items to strings
        items: list[str] = []
        items = self.load_runtime_arg_array(input_id, input_type)

        if "null" in input_type:
            return []
        elif "bool" in input_type:
            # NOTE: At the moment not sure how this should be implemented.
            # FIXME yaml safe_load converts yaml strings to likely Python types.
            # This is a problem here, as 'True' and 'true' both convert to the
            # Python bool type with value True.
            raise NotImplementedError()
        else:
            if itemSeparator:
                items = itemSeparator.join(items)
                if prefix and separate:         # -i= A,B,C
                    array_arg.append(prefix)
                    array_arg.append(items)
                elif prefix and not separate:   # -i=A,B,C
                    array_arg.append(prefix + items)
                else:                           # A,B,C
                    array_arg.append(items)
            else:
                if prefix and separate:         # -i= A B C
                    array_arg.append(prefix)
                    array_arg.extend(items)
                if prefix and not separate:     # -i=A -i=B -i=C
                    for item in items:
                        array_arg.append(prefix + item)
                else:                           # A B C
                    array_arg = items
        return array_arg

        
    def load_runtime_arg(self, input_id: str) -> str:
        """
        TODO
        """
        global_source_id: str = self.input_to_source[input_id]
        return str(self.runtime_context[global_source_id])


    def compose_arg(
            self,
            input_id: str,
            input_dict: dict[str, Any]
        ) -> list[str]:
        """
        Compose a single command-line argument.
        
        TODO desc
        TODO Support for optional arguments 
        TODO Support for bool type
        """
        args: list[str] = []
        prefix: str = ""
        separate: bool = True
        arg_type: str = input_dict["type"]

        if "prefix" in input_dict:
            prefix = input_dict["prefix"]
        if "separate" in input_dict:
            separate = input_dict["separate"]

        if "null" in arg_type:
            return []
        elif "bool" in arg_type:
            # FIXME yaml safe_load converts yaml strings to likely Python types.
            # This is a problem here, as 'True' and 'true' both convert to the
            # Python bool type with value True.
            
            # if prefix and self.runtime_context[arg_id]:
            #     args.append(prefix)
            # else:
            #     args.append(str(self.runtime_context))
            raise NotImplementedError()
        else:
            if separate:
                if prefix:
                    args.append(prefix)
                args.append(self.load_runtime_arg(input_id))
            else:
                args.append(prefix + self.load_runtime_arg(input_id))

        return args


    def compose_command(
            self,
            inputs: list[Tuple[str, dict[str, Any]]]
        ) -> list[str]:
        """
        TODO desc
        
        """
        cmds: list[str] = []
        for input_id, input_dict  in inputs:
            if "[]" in input_dict["type"]:
                # Compose array argument
                cmds.extend(self.compose_array_arg(input_id, input_dict))
            else:
                # Compose single argument
                cmds.extend(self.compose_arg(input_id, input_dict))
        return cmds
    

    def _process_outputs(self, result: bytes) -> None:

        for output_id, output_dict in self.outputs.items():
            # FIXME Checking formatting should probably not be done at runtime
            if "type" not in output_dict:
                raise Exception("Type missing from output in\n", self.id)

            # FIXME Checking formatting should probably not be done at runtime
            output_type: str = output_dict["type"]
            parent_process = self.loading_context["processes"][self.parent_process_id]
            global_output_id = parent_process.global_id(self.step_id + "/" + output_id)

            if "string" in output_type:
                self.runtime_context[global_output_id] = result.decode()
                print(result.decode())
            elif "file" in output_type:
                if not "glob" in output_dict:
                    raise Exception(f"No glob field in output {output_id}")
                
                value = self.eval(output_dict["glob"])
                print(value)
                output_file_paths = glob.glob(value)
                if "[]" in output_type:
                    # Output is an array of objects
                    self.runtime_context[global_output_id] = output_file_paths
                else:
                    # Output is a single object
                    self.runtime_context[global_output_id] = output_file_paths[0]
                print(output_file_paths)
            else:
                raise NotImplementedError(f"Output type {output_type} is not supported")

    

    def cmd_wrapper(
            self,
            inputs: list[Tuple[str, dict[str, Any]]],
            *parents
        ):
        """
        TODO desc
        Wrapper for subprocess.run().

        The wrapper is lazily executed in a dask.delayed task graph.
        """
        # TODO: Add support for:
        #     stdin
        #     stdout
        #     stderr

        # # Set-up working directory
        # main_path: Path = Path(self.main_path)
        # with chdir(main_path):
        #     working_directory: Path = main_path / self.short_id
        #     os.mkdir(working_directory)

        # with chdir(working_directory):
        
        # Turn inputs into arguments
        cmd: list[str] = self.compose_command(inputs)

        # Combine the base command with the arguments
        if hasattr(self, "base_command"):
            if isinstance(self.base_command, list):
                cmd = [*self.base_command] + cmd
            else:
                cmd = [self.base_command] + cmd

        # print("Current working directory:", os.getcwd())
        print("Executing:", " ".join(cmd))
        result: bytes = subprocess.run(
            cmd,
            # stdout=subprocess.PIPE,
            capture_output=True
        ).stdout
        # Process outputs if needed
        if self.step_id:
            self._process_outputs(result)
        else:
            print(result.decode())

        


    def create_task_graph(self, *parents) -> None:
        """
        Build a Dask Delayed object to execute the wrapped tool with.
        TODO desc
        """
        pos_inputs: list[Tuple[str, dict[str, Any]]] = []
        key_inputs: list[Tuple[str, dict[str, Any]]] = []

        # Decide whether this input argument is positional or not
        for input_id, input_dict in self.inputs.items():
            if hasattr(input_dict, "position"):
                pos_inputs.append((input_id, input_dict))
            else:
                key_inputs.append((input_id, input_dict))

        # Order input arguments
        inputs: list[Tuple[str, dict]] = sorted(pos_inputs, key=lambda x: x[1]["position"])
        inputs += key_inputs

        # "parents" parameter chains steps together
        self.task_graph_ref = dask.delayed(self.cmd_wrapper)(inputs, *parents)