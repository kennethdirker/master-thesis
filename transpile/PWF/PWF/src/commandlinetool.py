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
    cast,
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
PYTHON_CWL_T_MAPPING: dict[Type, List[str]] = {
    NoneType: ["null"],
    Absent: ["null"],
    bool: ["boolean"],
    int: ["int", "long"],
    float: ["float", "double"],
    str: ["string"],
    FileObject: ["file", "directory"]
}

"""
Mapping of CWL types to Python types. CWL supports types that base Python does not
recognize or support, like double and long. FIXME This is a band-aid for now.
"""
CWL_PYTHON_T_MAPPING: dict[str, Type] = {
    "null": NoneType,
    "boolean": bool,
    "int": int,
    "long": int,
    "float": float,
    "double": float,
    "string": str,
    "file": FileObject,
    "directory": FileObject,
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


    def prepare_runtime_context(self, cwl_namespace):
        """
        Complete runtime context input values if default or valueFrom are
        specified in the input parameter. Expressions are evaluated. 
        If an input parameter has no value (or None/"null") supplied from the 
        input object, default is used if specified. If 'valueFrom' is provided,
        it overrides the input object input and default value.

        Expressions in 'valueFrom' might reference 'self', which references the
        value gained from the input object or the 'default' field.
        TODO Test
        """
        for input_id, input_dict in self.inputs.items():
            # Get input value from runtime context
            global_source_id: str = self.input_to_source[input_id]
            value = self.runtime_context[global_source_id]

            # Check if the input has a value. If not, try to fill it with a
            # default value
            if isinstance(value, Absent | NoneType):
                # Input did not receive a non-None value from input object, try
                # to get a default value or from the an input binding valueFrom
                if "default" in input_dict and input_dict["default"] is not None:
                    value: Any = input_dict["default"]
                    if isinstance(value, str):
                        # String might be an expression and need evaluation.
                        value = self.eval(value, cwl_namespace)

            # Add value of 'self' to evaluation context.
            namespace: dict[str, Any] = cwl_namespace.copy()
            namespace.update({"self": value})

            # If provided, valueFrom gives value to the input parameter. 
            if "valueFrom" in input_dict and not isinstance(value, Absent | NoneType):
                # If the value of the associated input parameter is null,
                # valueFrom is not evaluated and nothing is added to the 
                # command line. This means we set value to None, which is
                # handled in 'build_commandline()'.
                if not isinstance(value, str):
                    raise Exception(f"'valueFrom' of {input_id} does not contain string")
                value = self.eval(input_dict["valueFrom"], namespace)
            else:
                # String might be an expression and need evaluation
                if isinstance(value, str):
                    value = self.eval(value, cwl_namespace)

            self.runtime_context[global_source_id] =  value


    def compose_array_arg(
            self,
            values: List[Any],
            value_t: str,
            input_dict: dict[str, Any]
        ) -> list[str]:
        """
        Compose a single command-line array argument.
        TODO Test
        """
        # null type and empty arrays do not add to the command line
        if value_t == "null" or len(values) == 0:
            return []

        # Set default properties as per CWL spec
        prefix: str = ""
        separate: bool = True
        itemSeparator: str | None = None

        # Load properties
        if "prefix" in input_dict:
            prefix = input_dict["prefix"]
        if "itemSeparator" in input_dict:
            itemSeparator = input_dict["itemSeparator"]
        if "separate" in input_dict:
            separate = input_dict["separate"]

        array_arg: list[str] = []

        if "boolean" in value_t:
            # NOTE: At the moment not sure how this should be implemented.
            # Would it look like: '--prefix True False True True' ?
            # FIXME yaml safe_load converts yaml strings to likely Python types.
            # This is a problem here, as 'True' and 'true' both convert to the
            # Python bool type with value True.
            raise NotImplementedError()
        else:
            if itemSeparator:
                items_joined = itemSeparator.join(values)
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
                    array_arg.extend(values)
                if prefix and not separate:     # -i=A -i=B -i=C
                    for item in values:
                        array_arg.append(prefix + item)
                else:                           # A B C
                    array_arg = values
        return array_arg

        
    def compose_arg(
            self,
            value: Any,
            value_t: str,
            input_dict: dict[str, Any],
        ) -> list[str]:
        """
        Compose a single command-line argument.
        TODO Test
        """
        # null type doesn't add to the command line
        if value_t == "null":
            return []
        
        # Set default properties as per CWL spec
        prefix: str = ""
        separate: bool = True

        if "prefix" in input_dict:
            prefix = input_dict["prefix"]
        if "separate" in input_dict:
            separate = input_dict["separate"]

        args: list[str] = []

        if value_t == "boolean" and "prefix" in input_dict:
            args.append(prefix)
        else:
            # Format argument
            if separate:
                if prefix:
                    args.append(prefix)
                args.append(value)
            else:
                args.append(prefix + value)

        return args


    def build_commandline(
            self, 
            # cwl_namespace: dict[str, Any]
        ) -> list[str]:
        """
        Build the command line to be executed. Takes values from the runtime
        context. If the runtime context holds None or the 'Absent' type for an
        input parameter, nothing is added to the command line.
        NOTE: Runtime context must be prepared before this function is called.
        TODO Test
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
                    # If the array is empty, it does not add anything to command line
                    continue

                # Check if all array elements have the same type.
                if any([not isinstance(v, value[0]) for v in value]):
                    raise Exception("Array is not homogeneous")

                expected_types = [t for t in expected_types if "[]" in t] # Filter for array types
                value_types = PYTHON_CWL_T_MAPPING[type(value[0])]
            else:
                # Dealing with single item.
                value_types = PYTHON_CWL_T_MAPPING[type(value)]

            # Check if the type of the received runtime input value is valid
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
            
            if isinstance(value, NoneType | Absent):
                if optional:
                    continue
                else:
                    raise Exception(f"Required input '{input_id}' has no value")
            
            value_type = cast(str, value_type)
            print(value)
            if is_array:
                # Compose argument with array data
                cmd.extend(self.compose_array_arg(value, value_type, input_dict))
            else:
                # Compose argument with single data value
                cmd.extend(self.compose_arg(value, value_type, input_dict))

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
        sets the outputs.
        
        NOTE: This function does not match the outputs with the output schema!
        In order words, the outputs are not validated here.

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

            # TODO Other output types, like int, float, etc 
            if "string" in output_type:
                # Get stdout from subprocess.run and decode to utf-8
                output[global_output_id] = completed.stdout.decode()
                output["stdout"] = completed.stdout.decode()
            elif "file" in output_type:
                # Generate an output parameter based on the files produced
                # by a CommandLineTool.
                if "glob" in output_dict:
                    glob_string: str = self.eval(output_dict["glob"], cwl_namespace)
                    output_file_paths: List[str] = glob.glob(glob_string)

                    if len(output_file_paths) == 0:
                        raise Exception(f"Output glob {glob_string} (from '{output_dict['glob'][1:-1]}') did not match any files")

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
        self.prepare_runtime_context(cwl_namespace)
        cmd: list[str] = self.build_commandline()

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

        # TODO Validate outputs
        outputs_schema: List[dict] = []
        # TODO Transform self.outputs (whether single item, list, or dict) into outputs_schema list
        # TODO Check if outputs match schema
        # TODO Evaluate valueFrom NOTE: Should this happen here or in the wrapper?
        
        # Print stderr/stdout
        # FIXME Check if this works
        if "stdout" in new_state:
            print(new_state["stdout"], file=sys.stdout)

        if "stderr" in new_state:
            print(new_state["stderr"], file=sys.stderr)

        return new_state