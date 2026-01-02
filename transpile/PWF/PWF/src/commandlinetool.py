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
    Sequence,
    Tuple,
    Type,
    Union
)

from .process import BaseProcess
from .utils import Absent, FileObject, DirectoryObject, Value, PYTHON_CWL_T_MAPPING, CWL_PYTHON_T_MAPPING


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
        # self.set_metadata()
        # self.set_inputs()
        # self.set_outputs()
        # self.set_requirements()
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
            value = self.runtime_context[global_source_id].value

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
            if "valueFrom" in input_dict and isinstance(value, Value):
            # if "valueFrom" in input_dict and not isinstance(value, Absent | NoneType):
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

            self.runtime_context[global_source_id] =  Value(value, type(value), PYTHON_CWL_T_MAPPING[type(value)][0])


    def match_inputs(self):
        """
        Check if the input values match to the input schema in self.inputs. If 
        not all inputs match, an exception is raised.
        
        """



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
        
        # Transform values into strings
        values_s: List[str] = [str(v) for v in values]

        # Format the argument(s) according to prefix and separator config
        if itemSeparator:
            items_joined = itemSeparator.join(values_s)
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
                array_arg.extend(values_s)
            if prefix and not separate:     # -i=A -i=B -i=C
                for item in values_s:
                    array_arg.append(prefix + item)
            else:                           # A B C
                array_arg = values_s
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
            # Stringify value
            value_s = str(value)
            # Format argument
            if separate:
                if prefix:
                    args.append(prefix)
                args.append(value_s)
            else:
                args.append(prefix + value_s)

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
            if "bound" in input_dict and input_dict["bound"]:
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
            value = self.runtime_context[global_source_id].value

            # Load expected input types
            expected_types: list[str] = []
            # _t: Type = type(input_dict["type"])
            if isinstance(input_dict["type"], str):
                expected_types = [input_dict["type"]]
            elif isinstance(input_dict["type"], list):
                expected_types = input_dict["type"]
            else:
                raise Exception(f"Unexpected type(input_dict['type']). Should be 'str' or 'list', but found '{type(input_dict['type'])}'")
                # raise Exception(f"Unexpected type(input_dict['type']). Should be 'str' or 'list', but found '{_t}'")

            # Check whether we are dealing with an array or not
            is_array: bool = False
            if isinstance(value, list):
                # Value is an array
                is_array = True

                if len(value) == 0:
                    # If the array is empty, it does not add anything to command line
                    continue

                # Check if all array elements have the same type.
                if any([not isinstance(v, type(value[0])) for v in value]):
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
                    if is_array:
                        if v_type + "[]" == valid_type:
                            value_type = v_type + "[]"
                    else:
                        if v_type == valid_type:    # Exact string match
                            value_type = v_type

            optional = False
            if value_type is None:
                # No match, check if the input is optional. If so, don't add
                # anything to the command line
                if not "null" in expected_types:
                    raise Exception(f"Tool input '{input_id}' supports CWL types [{', '.join(expected_types)}], but found CWL types ['{', '.join(value_types)}'] (from '{type(value)}')")
                optional = True
            
            if isinstance(value, NoneType | Absent):
                if optional:
                    continue
                else:
                    raise Exception(f"Required input '{input_id}' has no value")
            
            value_type = cast(str, value_type)

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
            cwl_namespace: dict[str, Any],
            output_schema: dict[str, Any],
            env: dict[str, Any]
        ) -> dict[str, Value]:
        """
        Wrapper for subprocess.run(). Executes the command line tool and
        sets the outputs.

        TODO Wrap output values in Value object.
        TODO Add support for all CWL datatypes
        TODO Correct type checking
        
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

        # Match outputs against the output schema and create outputs
        # based on output bindings.


        # Capture stdout and stderr from command. 
        # NOTE: We are not printing them here, because we might not be on the
        # main thread when using Dask.
        # TODO Maybe wrap in Value object (later on)?
        output: dict = {
            "stdout": completed.stdout,
            "stderr": completed.stderr
        }
        
        # Process outputs.
        # Command outputs are matched against the tool's output schema. Tool 
        # outputs are generated based on the output bindings defined in the
        # output schema.
        for output_id, output_dict in output_schema.items():
            global_output_id = self.global_id(output_id)
            
            # Get types
            output_types: List[str]
            if "type" not in output_dict:
                raise Exception(f"Output {output_id} is missing type")
            if isinstance(output_dict["type"], List):
                output_types = output_dict["type"]
            elif isinstance(output_dict["type"], str):
                output_types = [output_dict["type"]]
            else:
                raise Exception(f"Expected 'str' or 'list' in output type, but found '{type(output_dict['type'])}'")
            
            # glob
            if "glob" in output_dict and output_dict["glob"] is not None:
                # Evaluate glob patterns. Is either a string, expression or
                # array of strings. First normalize to list of 
                # glob strings, then capture matching files.
                globs: List[str]

                if isinstance(output_dict["glob"], str):
                    # Evaluate string and check if the output is either a
                    # string or list of strings.
                    eval_return: Any = self.eval(output_dict["glob"], cwl_namespace)
                    if isinstance(eval_return, str):
                        globs = [eval_return]
                    elif isinstance(eval_return, List):
                        globs = eval_return
                    else:
                        raise Exception(f"Expression of '{output_id}' did not evaluate to 'str' or 'list', but '{type(eval_return)}'")
                elif not isinstance(output_dict["glob"], List):
                    raise Exception(f"Output glob of '{output_id}' is neither 'str' nor 'list', but '{type(output_dict['glob'])}'")
                
                globs = output_dict["glob"]
                
                # Validate that list of globs contains strings
                for g in globs:
                    if isinstance(g, str):
                        raise Exception(f"Output '{output_id}' has glob '{g}' that should be 'str', but is '{type(g)}'")
                    
                # Collect matched files
                output_file_paths: List[str] = []
                for g in globs:
                    output_file_paths.extend(glob.glob(g, root_dir = self.loading_context["designated_out_dir"]))
                
                # Set 'self' for outputEval and loadContents
                cwl_namespace["self"] = output_file_paths


            # loadContents
            if ("loadContents" in output_dict and output_dict["outputEval"] is not None):
                if not any(["file" in type_ for type_ in output_types]):
                    raise Exception(f"loadContents must only be used on 'file' or 'file[]' type")
                # TODO
                raise NotImplementedError()

            # outputEval
            if (
                "outputEval" in output_dict and 
                output_dict["outputEval"] is not None
            ):
                # Evaluated expression might yield any type, so validation is needed
                value = self.eval(output_dict["outputEval"], cwl_namespace)
                # TODO Validate value type
                # NOTE: For now, just force
                
                
                # Wrap in Value object and put in output
                # FIXME Handle this logic in Value constructor? Also at other spots in code.
                if isinstance(value, Sequence):
                    output[global_output_id] = Value(value, type(value[0]), PYTHON_CWL_T_MAPPING[type(value[0])][0])
                else:
                    output[global_output_id] = Value(value, type(value), PYTHON_CWL_T_MAPPING[type(value)][0])

            # secondaryFiles
            if ("secondaryFiles" in output_dict and output_dict["outputEval"] is not None):
                if not any(["file" in type_ for type_ in output_types]):
                    raise Exception(f"secondaryFiles must only be used on 'file' or 'file[]' type")
                # TODO Check if actual output is file or file[]
                raise NotImplementedError()
                # if isinstance(output[global_output_id], List): 
                #     output[global_output_id].extend(Value(...))
                # else:
                #     ???
            
        return output

        
    def execute(
            self, 
            use_dask: bool,
            runtime_context: Optional[dict[str, Any]] = None,
            verbose: Optional[bool] = True,
            client: Optional[Client] = None,
        ) -> dict[str, Value]:
        """
        TODO Description
        TODO Validate outputs
        """
        # Update runtime context
        if runtime_context is not None:
            self.runtime_context = runtime_context


        # Build the command line
        cwl_namespace = self.build_namespace()
        self.prepare_runtime_context(cwl_namespace)
        self.match_inputs()
        cmd: list[str] = self.build_commandline()

        # Build the execution environment
        env = self.build_env(cwl_namespace)

        # Submit and execute tool and gather output
        new_state: dict[str, Value] = {}
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

        # Print stderr/stdout
        # FIXME TODO Redirect to configured stdout/stderr
        if "stdout" in new_state:
            print(new_state["stdout"], file=sys.stdout)

        if "stderr" in new_state:
            print(new_state["stderr"], file=sys.stderr)

        return new_state