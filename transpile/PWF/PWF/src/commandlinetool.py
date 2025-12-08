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

from types import NoneType
from typing import (
    Any,
    Callable,
    List,
    TextIO,
    Optional,
    Tuple,
    Type,
    Union
)

from .process import BaseProcess
from .utils import Absent, FileObject

    
"""
Mapping of Python types to CWL types. CWL supports types that base Python does
not recognize or support, like double and long. FIXME This is a band-aid for now.
"""
TYPE_MAPPING: dict[Type, List[str]] = {
    NoneType: ["null"],
    Absent: ["null"],
    bool: ["boolean"],
    int: ["int", "long"],
    float: ["float", "double"],
    str: ["string"],
    FileObject: ["file", "directory"]
}


class BaseCommandLineTool(BaseProcess):

    def __init__(
            self,
            main: bool = True,
            runtime_context: Optional[dict] = None,
            loading_context: Optional[dict[str, str]] = None,
            parent_process_id: Optional[str] = None,
            step_id: Optional[str] = None,
            with_dask: bool = False,
            PATH: Optional[str] = None,
        ):
        """ TODO: class description """

        # Initialize BaseProcess class
        super().__init__(
            main = main,
            runtime_context = runtime_context,
            loading_context = loading_context,
            parent_process_id = parent_process_id,
            step_id = step_id
        )

        # Prepare properties
        self.base_command: list[str] | str | None = None
        if PATH is None:
            PATH = os.environ.get("PATH", "")
        # self.env: dict[str, Any] = {}

        # Digest CommandlineTool file
        self.set_metadata()
        self.set_inputs()
        self.set_outputs()
        self.set_requirements()
        self.set_base_command()
        
        if main:
            self.register_input_sources()
            self.execute(self.loading_context["use_dask"], verbose=True)


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

        arg_types: list[str] = []
        if isinstance(input_dict["type"], str):
            arg_types = [input_dict["type"]] 
        elif isinstance(input_dict["type"], list):
            arg_types = input_dict["type"]
        else:
            raise Exception(f"Caught unexpected input type for input '{input_id}'")
        
        print("[TOOL] COMPOSE_ARRAY_ARG", f"{input_id}: {arg_types}")

        # Set default properties as per CWL spec
        prefix: str = ""
        separate: bool = True
        itemSeparator: str | None = None

        # Load properties
        arg_types = ["".join(c for c in t if c not in "[]?") for t in arg_types]   # Filter []? from type string
        if "prefix" in input_dict:
            prefix = input_dict["prefix"]
        if "itemSeparator" in input_dict:
            itemSeparator = input_dict["itemSeparator"]
        if "separate" in input_dict:
            separate = input_dict["separate"]

        # Convert array items to strings
        items: list[str] = []
        items = self.load_runtime_arg_array(input_id, arg_types, cwl_namespace)
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
        ) -> Any | Absent | None:
        """
        TODO Where do we evaluate now???

        TODO valueFrom?
        """
        global_source_id: str = self.input_to_source[input_id]
        value = self.runtime_context[global_source_id]
        if isinstance(value, str):
            value = self.eval(value, cwl_namespace)
        if isinstance(value, list):
            # If dealing with array data, just take the first item for now FIXME
            value = value[0]
        return value


    def compose_arg(
            self,
            value: Any,
            value_t: str | None,
            input_id: str,
            input_dict: dict[str, Any],
            cwl_namespace: dict[str, Any],
            optional: bool = False
        ) -> list[str]:
        """
        Compose a single command-line argument.
        
        TODO desc, how is value chosen?
        TODO Support for optional arguments 
        TODO Support for bool type
        """
        args: list[str] = []
        # Set default properties as per CWL spec
        prefix: str = ""
        separate: bool = True

        if "prefix" in input_dict:
            prefix = input_dict["prefix"]
        if "separate" in input_dict:
            separate = input_dict["separate"]

        # Check whether the argument has a default value to be used or the 
        # command line argument is optional.
        # Raise an exception if the input is mandatory, but missing.
        if isinstance(value, (Absent, type(None))):
            if "default" in input_dict:
                # TODO FIXME Do this somewhere else?
                value = input_dict["default"]
            else:
                if optional:
                    # Input is optional, so we just ignore this argument
                    return []
                else:
                    raise Exception(f"Input {input_id} is missing")

        if value_t == "boolean":
            # FIXME yaml safe_load converts yaml strings to likely Python types.
            # This is a problem here, as 'True' and 'true' both convert to the
            # Python bool type with value True.
            raise NotImplementedError()
        else:
            # Format the 
            if separate:
                if prefix:
                    args.append(prefix)
                args.append(value)
            else:
                args.append(prefix + value)

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

        # TODO FIXME
        # Aside from 'baseCommand' and 'inputs', we need to parse 'arguments'!

        # Split positional arguments and key arguments
        for input_id, input_dict in self.inputs.items():
            # Skip unbound (without input binding) inputs
            if hasattr(input_dict, "bound") and input_dict["bound"]:
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
            # Load value from runtime_context
            global_source_id: str = self.input_to_source[input_id]
            value = self.runtime_context[global_source_id]

            # Load expected input types
            expected_types: list[str] = []
            _t: Type = type(input_dict["type"])
            if isinstance(_t, str):
                expected_types = [input_dict["type"]]
            elif isinstance(_t, list):
                expected_types = input_dict["type"]
            else:
                raise Exception(f"Unexpected type(input_dict['type']). Should be 'str' or 'list', but found '{_t}'")

            # Check whether we are dealing with an array or not
            is_array: bool = False
            if isinstance(value, list):
                # Value is an array
                is_array = True

                if len(value) == 0:
                    # TODO Should empty arrays be supported?
                    raise NotImplementedError(f"Found empty array for input '{input_id}'")
                
                # Check if all array elements have the same type.
                if any([not isinstance(v, value[0]) for v in value]):
                    raise Exception("Array is not homogeneous")
                 
                expected_types = [t for t in expected_types if "[]" in t] # Filter for array types
                value_types = TYPE_MAPPING[type(value[0])]
            else:
                # Dealing with single item.
                value_types = TYPE_MAPPING[type(value)]
                
            # Check if the type of the received runtime input value is
            value_type: str | None = None
            for v_type in value_types:
                for valid_type in expected_types:
                    if v_type == valid_type:    # Exact string match
                        value_type = v_type
            optional = False
            if value_type is None:
                # No match, check if the input is optional. If so, don't add
                # anything to the command line
                if not "null" in expected_types:
                    raise Exception(f"Tool input '{input_id}' supports CWL types '[{', '.join(expected_types)}], but found CWL types ['{', '.join(value_types)}'] (from '{type(value)}')")
                optional = True

            # TODO 'value' is not evaluated yet. Where to do this?
            if is_array:
                # Compose argument with array data
                # TODO FIXME UPDATE
                cmd.extend(self.compose_array_arg(input_id, input_dict, cwl_namespace))
            else:
                # Compose argument with single data value
                cmd.extend(self.compose_arg(value, value_type, input_id, input_dict, cwl_namespace, optional))

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
    

    def build_env(self, cwl_namespace) -> dict[str, Any]:
        """
        Build the execution environment for the command line tool. The 
        environment is new dictionary filled as follows:
        - HOME must be set to the designated output directory.
        - TMPDIR must be set to the designated temporary directory.
        - PATH may be inherited from the parent process, except when TODO run in a container that provides its own PATH.
        - Variables defined by EnvVarRequirement
        - TODO The default environment of the container, such as when using DockerRequirement

        Returns:
            A dictionary representing the environment variables.
        """
        env: dict[str, Any] = {
            "HOME": self.loading_context["designated_out_dir"],
            "TMPDIR": self.loading_context["designated_tmp_dir"],
            "PATH": self.loading_context["PATH"],
        }

        # Add variables from EnvVarRequirement
        if "EnvVarRequirement" in self.requirements:
            for key, value in self.requirements["EnvVarRequirement"].items():
                env[key] = self.eval(value, cwl_namespace)

        return env


    def run_wrapper(
            self,
            cmd: list[str],
            cwl_namespace,
            outputs: dict[str, Any],
            env: dict[str, Any]
        ) -> dict[str, Any]:
        """
        Wrapper for subprocess.run(). Executes the command line tool and
        processes the outputs.

        Returns:
            A dictionary containing all newly added runtime output state.
        """
        # Execute tool
        print("[TOOL]: EXECUTING:", " ".join(cmd))
        completed: CompletedProcess = run(
            cmd,
            capture_output=True,
            env = env
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
        # TODO Evaluate / fill all inputs that rely on expressions (check default/valueFrom?)
        cmd: list[str] = self.build_commandline(cwl_namespace)

        # Evaluate expressions in outputs. This has to be done before wrapper 
        # execution, FIXME but I cant remember why.
        for output_id, output_dict in self.outputs.items():
            for property, value in output_dict.items():
                self.outputs[output_id][property] = self.eval(value, cwl_namespace)

        # Build the execution environment
        env = self.build_env(cwl_namespace)

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
                env,
                pure = False
            )
            new_state = future.result()
        else:
            new_state = self.run_wrapper(
                cmd,
                cwl_namespace,
                self.outputs,
                env
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