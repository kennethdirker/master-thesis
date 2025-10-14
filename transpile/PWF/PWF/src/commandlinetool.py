# import dask.delayed
import glob
import os
# import subprocess
import sys

from abc import abstractmethod
from contextlib import chdir
from dask.distributed import Client
from pathlib import Path
from subprocess import run, CompletedProcess
from typing import Any, Callable, TextIO, Optional, Tuple, Union

from .process import BaseProcess

class BaseCommandLineTool(BaseProcess):

    def __init__(
            self,
            main: bool = True,
            runtime_context: Optional[dict] = None,
            loading_context: Optional[dict[str, str]] = None,
            parent_process_id: Optional[str] = None,
            step_id: Optional[str] = None,
            with_dask: bool = False
        ):
        """ TODO: class description """
        super().__init__(
            main = main,
            runtime_context = runtime_context,
            loading_context = loading_context,
            parent_process_id = parent_process_id,
            step_id = step_id
        )

        # Prepare properties
        self.base_command: list[str] | str | None = None

        # Digest CommandlineTool file
        self.set_metadata()
        self.set_inputs()
        self.set_outputs()
        self.set_requirements()
        self.set_base_command()
        
        if main:
            self.register_input_sources()
            self.execute(with_dask, verbose=True)


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
        TODO Explain why this is needed.
        Link local process input IDs to global input IDs.
        NOTE: Only executed if CommandLineTool is called as main.
        """
        if not self.is_main:
            raise Exception("register_input_sources should be called from main process")
        
        for input_id in self.inputs:
            self.input_to_source[input_id] = self.global_id(input_id)


    def load_runtime_arg_array(
            self,
            input_id: str,
            input_type: str,
            cwl_namespace: dict[str, Any]   #TODO Use to evaluate expressions
        ) -> list[str]:
        """
        TODO Expression evaluation in array items
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
            # TODO FIXME Evaluate expressions in array items needed?

            global_source_id: str = self.input_to_source[input_id]
            value = self.runtime_context[global_source_id]
            if isinstance(value, list):
                args = [str(item) for item in value]
            else:
                args = [str(value)]
        return args


    def compose_array_arg(
            self,
            input_id: str,
            input_dict: dict[str, Any],
            cwl_namespace: dict[str, Any]
        ) -> list[str]:
        """
        Compose a single command-line array argument.

        TODO desc
        TODO Support for optional arguments 
        TODO Support for bool type
        """
        array_arg: list[str] = []
        input_type: str = input_dict["type"]
        print("[TOOL] COMPOSE_ARRAY_ARG", f"{input_id}: {input_type}")

        # Set default properties as per CWL spec
        prefix: str = ""
        separate: bool = True
        itemSeparator: str | None = None

        # Load properties
        input_type = "".join(c for c in input_type if c not in "[]?")   # Filter []? from type string
        if "prefix" in input_dict:
            prefix = input_dict["prefix"]
        if "itemSeparator" in input_dict:
            itemSeparator = input_dict["itemSeparator"]
        if "separate" in input_dict:
            separate = input_dict["separate"]

        # Convert array items to strings
        items: list[str] = []
        items = self.load_runtime_arg_array(input_id, input_type, cwl_namespace)
        print("[TOOL] ARRAY ITEMS: ", items, f"({type(items)})")
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
                items_joined = itemSeparator.join(items)
                if prefix and separate:         # -i= A,B,C
                    array_arg.append(prefix)
                    array_arg.append(items_joined)
                elif prefix and not separate:   # -i=A,B,C
                    array_arg.append(prefix + items_joined)
                else:                           # A,B,C
                    array_arg.append(items_joined)
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

        
    def load_runtime_arg(
            self, 
            input_id: str,
            cwl_namespace: dict[str, Any]
        ) -> str:
        """
        TODO
        """
        global_source_id: str = self.input_to_source[input_id]
        value = self.runtime_context[global_source_id]
        if isinstance(value, str):
            value = self.eval(value, cwl_namespace)
        if isinstance(value, list):
            # If dealing with array data, just take the first item for now FIXME
            value = value[0]
        return str(value)


    def compose_arg(
            self,
            input_id: str,
            input_dict: dict[str, Any],
            cwl_namespace: dict[str, Any]
        ) -> list[str]:
        """
        Compose a single command-line argument.
        
        TODO desc
        TODO Support for optional arguments 
        TODO Support for bool type
        """
        args: list[str] = []
        # Set default properties as per CWL spec
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
                args.append(self.load_runtime_arg(input_id, cwl_namespace))
            else:
                args.append(prefix + self.load_runtime_arg(input_id, cwl_namespace))

        return args


    def build_commandline(
            self, 
            cwl_namespace: dict[str, Any]
        ) -> list[str]:
        """
        Build the command line to be executed.
        """
        pos_inputs: list[Tuple[str, dict[str, Any]]] = []
        key_inputs: list[Tuple[str, dict[str, Any]]] = []

        # Split positional arguments and key arguments
        for input_id, input_dict in self.inputs.items():
            if hasattr(input_dict, "position"):
                pos_inputs.append((input_id, input_dict))
            else:
                key_inputs.append((input_id, input_dict))

        # Order the arguments
        inputs: list[Tuple[str, dict]] = sorted(pos_inputs, key=lambda x: x[1]["position"])
        inputs += key_inputs

        # Match arguments with runtime input values
        cmd: list[str] = []
        for input_id, input_dict  in inputs:
            if "[]" in input_dict["type"]:
                # Compose array argument
                cmd.extend(self.compose_array_arg(input_id, input_dict, cwl_namespace))
            else:
                # Compose single argument
                cmd.extend(self.compose_arg(input_id, input_dict, cwl_namespace))

        # Combine the base command with the arguments
        if hasattr(self, "base_command"):
            if isinstance(self.base_command, list):
                cmd = [*self.base_command] + cmd
            elif isinstance(self.base_command, str):
                cmd = [self.base_command] + cmd
            else:
                raise Exception(f"base_command should be 'str' or 'list[str]',"
                                f"but found '{type(self.base_command)}'")
        return cmd


    def run_wrapper(
            self,
            cmd: list[str],
            cwl_namespace,
            outputs: dict[str, Any]
        ) -> dict[str, Any]:
        """
        Wrapper for subprocess.run(). Executes the command line tool and
        processes the outputs.

        Returns:
            A dictionary containing all newly added runtime output state.
        """
        # Execute tool
        print("[TOOL]: EXECUTING", " ".join(cmd))
        completed: CompletedProcess = run(
            cmd,
            capture_output=True
        )

        # Capture stderr
        output: dict = {"stderr": completed.stderr.decode()}
        
        # Process tool outputs
        for output_id, output_dict in outputs.items():
            global_output_id = self.global_id(output_id)
            if "type" not in output_dict:
                raise Exception("Type missing from output in\n", self.id)
            output_type: str = output_dict["type"]

            if "string" in output_type:
                # Get stdout from subprocess.run and decode to utf-8
                output[global_output_id] = completed.stdout.decode()
                output["stdout"] = completed.stdout.decode()
            elif "file" in output_type:
                # Generate an output parameter based on the files produced
                # by a CommandLineTool.
                if "glob" in output_dict:
                    glob_string: str = self.eval(output_dict["glob"], cwl_namespace)
                    output_file_paths = glob.glob(glob_string)

                    if len(output_file_paths) == 0:
                        raise Exception(f"Output glob {glob_string} (from '{output_dict['glob'][1:-1]}') did not match any files")

                    # output_file_paths = glob.glob(output_dict["glob"])
                    if "[]" in output_type:
                        # Output is an array of objects
                        output[global_output_id] = output_file_paths
                    else:
                        # Output is a single object
                        output[global_output_id] = output_file_paths[0]
                elif "loadContents" in output_dict:
                    raise NotImplementedError()
                elif "outputEval" in output_dict:
                    raise NotImplementedError()
                elif "secondaryFiles" in output_dict:
                    raise NotImplementedError()
                else:
                    raise Exception(f"No method to resolve output schema:{output_dict}")
            else:
                raise NotImplementedError(f"Output type {output_type} is not supported")
        return output

        
    def execute(
            self, 
            use_dask: bool,
            runtime_context: Optional[dict[str, Any]] = None,
            verbose: Optional[bool] = True,
            client: Optional[Client] = None,
        ) -> dict[str, Any]:
        """
        TODO Description
        """
        # Update runtime context
        if runtime_context is not None:
            self.runtime_context = runtime_context

        # Build the command line
        cwl_namespace = self.build_namespace()
        cmd: list[str] = self.build_commandline(cwl_namespace)

        # Evaluate expressions in outputs. This has to be done before wrapper 
        # execution, FIXME but I cant remember why.
        for output_id, output_dict in self.outputs.items():
            for property, value in output_dict.items():
                self.outputs[output_id][property] = self.eval(value, cwl_namespace)


        # Submit and execute tool and gather output
        new_state: dict[str, Any] = {}
        if use_dask:
            if client is None:
                client = Client()
            
            future = client.submit(
                self.run_wrapper,
                cmd,
                cwl_namespace,
                self.outputs,
                pure = False
            )
            new_state = future.result()
        else:
            new_state = self.run_wrapper(
                cmd,
                cwl_namespace,
                self.outputs,
            )

        # TODO Check if all outputs are present
        
        # Print stderr/stdout
        # FIXME Check if this works
        if verbose:
            if "stdout" in new_state:
                print(new_state["stdout"], file=sys.stdout)
            if "stderr" in new_state:
                print(new_state["stderr"], file=sys.stderr)

        return new_state