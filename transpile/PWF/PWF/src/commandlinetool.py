# import dask.delayed
import glob
import os
# import subprocess
import shutil
import sys

from abc import abstractmethod
from contextlib import chdir
from dask.distributed import Client
from pathlib import Path
from subprocess import run, CompletedProcess
from uuid import uuid4

from types import NoneType
from typing import (
    Any,
    AnyStr,
    Callable,
    cast,
    IO,
    List,
    TextIO,
    Optional,
    Tuple,
    Type,
    Union
)

from .process import BaseProcess
from .utils import Absent, FileObject, DirectoryObject, Value, PY_CWL_T_MAPPING, CWL_PY_T_MAPPING


class BaseCommandLineTool(BaseProcess):
    # Tool info
    io: dict[str, Any]
    base_command: list[str] | str | None

    def __init__(
            self,
            main: bool = True,
            runtime_context: Optional[dict] = None,
            loading_context: Optional[dict[str, str]] = None,
            parent_process_id: Optional[str] = None,
            step_id: Optional[str] = None,
            inherited_requirements: Optional[dict[str, Any]] = None,
            PATH: Optional[str] = None,
        ):
        """ TODO: class description """

        # Initialize BaseProcess class
        super().__init__(
            main = main,
            runtime_context = runtime_context,
            loading_context = loading_context,
            parent_process_id = parent_process_id,
            step_id = step_id,
            # requirements = requirements,
        )

        # Prepare properties
        self.base_command = None
        if PATH is None:
            PATH = os.environ.get("PATH", "")

        # Digest CommandlineTool file
        self.set_base_command()
        self.set_io()
        self.process_requirements(inherited_requirements)
        
        if main:
            self.register_input_sources()
            outputs = self.execute(self.loading_context["use_dask"], verbose=True)
            self.finalize(outputs)


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
        Set the tool's base command.

        Implementations should assign ``self.base_command`` to either a
        string (single command) or a list of strings (command plus fixed
        arguments). This method is called during initialization so the
        instance can later combine the base command with runtime arguments
        produced by ``build_commandline()``.

        Example implementations:
            - ``self.base_command = "ls -l"``
            - ``self.base_command = ["ls", "-l"]``
        """
        pass


    def set_io(self) -> None:
        """
        Set the tool's IO options.
        """
        pass


    def process_requirements(
            self,
            inherited_requirements: dict[str, Any] | None
        ) -> None:
        """
        Set the requirements dict with tool-compatible inhertited requirements
        and override them if present in this tool.
        """
        if inherited_requirements is None:
            return
        
        TOOL_REQS = (
            "InlineJavascriptRequirement",
            "SchemaDefRequirement",
            "DockerRequirement",
            "SoftwareRequirement",
            "InitialWorkDirRequirement",
            "EnvVarRequirement",
            "ShellCommandRequirement",
            "ResourceRequirement",
            "LoadListingRequirement",
            "WorkReuse",
            "InplaceUpdateRequirement",
            "ToolTimeLimit"
        )

        updated_reqs = {}
        for req_key, req_dict in inherited_requirements.items():
            if req_key in TOOL_REQS:
                updated_reqs[req_key] = req_dict
        for req_key, req_body in self.requirements.items():
            updated_reqs[req_key] = req_body
        self.requirements = updated_reqs


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


    def stage_initial_work_dir_requirement(self, tmp_path: Path) -> None:
        # Stage files from InitialWorkDirRequirement
        if "InitialWorkDirRequirement" in self.requirements:
            ...
        # TODO


    def stage_input_files(self, tmp_path: Path) -> None:
        """
        """
        # Stage files and directories from input object
        for input_id, input_dict in self.inputs.items():
            types = input_dict["type"]
            if not isinstance(types, List):
                types = [types]
            path_likes = ["file", "file[]", "directory", "directory[]"]
            if not any(t in path_likes for t in types):
                continue

            source = self.input_to_source[input_id]
            value_wrapper = self.runtime_context[source]
            if isinstance(value_wrapper, Absent):
                continue

            type_: Type = value_wrapper.type
            if type_ not in (FileObject, DirectoryObject):
                # TODO FIXME Error or skip?
                continue 
            
            is_dir = type_ == DirectoryObject

            path_objects = value_wrapper.value
            if not value_wrapper.is_array:
                path_objects = [path_objects]
 
            tmp_path_objects = []
            for path_obj in path_objects:
                tmp_file_path = tmp_path / Path(path_obj.path).name
                
                # Writable files need to be copied to prevent overwriting.
                # For read-only files creating a symlink suffices.
                if "writable" in input_dict:
                    if is_dir:
                        shutil.copytree(path_obj.path, tmp_file_path)
                    else:
                        shutil.copy(path_obj.path, tmp_file_path)
                else:
                    tmp_file_path.symlink_to(path_obj.path, is_dir)

                # Save new path
                tmp_path_objects.append(type_(tmp_file_path))

            # Flatten if needed
            if not value_wrapper.is_array:
                tmp_path_objects = tmp_path_objects[0]

            new_value = Value(tmp_path_objects, type_, PY_CWL_T_MAPPING[type_][0])
            self.runtime_context[source] = new_value


    def stage_files(
            self, 
            tmp_path: Path, 
        ) -> None:
        """
        TODO Description
        """
        self.stage_initial_work_dir_requirement(tmp_path)
        self.stage_input_files(tmp_path)


    def prepare_runtime_context(self, cwl_namespace: dict[str, Any]):
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

            value_t = type(value[0]) if isinstance(value, List) else type(value)
            self.runtime_context[global_source_id] =  Value(value, value_t, PY_CWL_T_MAPPING[value_t][0])


    # def match_inputs(self):
    #     """
    #     Check if the input values match to the input schema in self.inputs. If 
    #     not all inputs match, an exception is raised.
    #     """



    def compose_array_arg(
            self,
            values: Any | List[Any],
            cwl_value_t: str,
            input_dict: dict[str, Any]
        ) -> list[str]:
        """
        Compose command-line tokens for an array-typed CWL input.

        This function converts a single value or a list of values into the
        appropriate sequence of command-line tokens according to CWL's
        InputBinding fields supported here: ``prefix``, ``separate`` and
        ``itemSeparator``.

        Args:
            values: A single item or list of items to be represented on the
                command line. If a non-list is provided it will be wrapped
                into a one-element list.
            cwl_value_t: The CWL runtime type string for the items (for
                example: ``"string"``, ``"file"``, ``"int"``. If the type
                is ``"null"`` or the list is empty, an empty list is
                returned.
            input_dict: The CWL input definition (inputBinding) which may
                contain keys ``prefix``, ``separate`` and ``itemSeparator``.

        Returns:
            A list of string tokens to append to the final command. The
            returned tokens reflect the combination rules for prefix,
            separation and item joining per CWL semantics.

        Raises:
            NotImplementedError: boolean array handling is not implemented.
        """
        # Wrap single item in list if needed
        if not isinstance(values, List):
            values = [values]

        # null type and empty arrays do not add to the command line
        if cwl_value_t == "null" or len(values) == 0:
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

        if "boolean" in cwl_value_t:
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
            value: Any | List[Any],
            cwl_value_t: str,
            input_dict: dict[str, Any],
        ) -> list[str]:
        """Compose command-line tokens for a single CWL input value.

        If ``value`` is provided as a list, the first element (head) is
        used — an empty list is treated as absent and yields an empty
        result. The function respects the inputBinding keys ``prefix`` and
        ``separate`` to produce either separate tokens or a concatenated
        token.

        Args:
            value: A single value or list; lists use only the first element
                (empty lists return ``[]``).
            cwl_value_t: The CWL type string describing the value (e.g.
                ``"string"``, ``"boolean"``, ``"file"``). If this is
                ``"null"`` an empty list is returned.
            input_dict: The CWL input binding dictionary, may contain
                ``prefix`` and ``separate``.

        Returns:
            A list of command-line tokens representing the argument, or an
            empty list if the value is considered absent.

        Notes:
            Booleans with prefix act as flags.
        """
        # Head of array will be used to format the argument
        if isinstance(value, List):
            # Empty array means no value (equal to "null")
            if len(value) == 0:
                return []
            value = value[0]

        # null type doesn't add to the command line
        if cwl_value_t == "null":
            return []
        
        # Set default properties as per CWL spec
        prefix: str = ""
        separate: bool = True

        if "prefix" in input_dict:
            prefix = input_dict["prefix"]
        if "separate" in input_dict:
            separate = input_dict["separate"]

        args: list[str] = []

        if cwl_value_t == "boolean" and "prefix" in input_dict:
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


    def build_commandline(self) -> list[str]:
        """Construct the full command line for the tool from runtime input values.

        The method iterates over the tool's inputs, orders positional
        parameters by their ``position``, validates runtime types against the
        declared CWL types, and delegates formatting to
        ``compose_array_arg`` or ``compose_arg``. If ``self.base_command`` is
        set it is prepended to the generated argument list.

        Precondition:
            ``prepare_runtime_context()`` must have been called so that
            ``self.runtime_context`` contains evaluated ``Value`` instances.

        Returns:
            A list of string tokens representing the full command to be
            executed.

        Raises:
            Exception on missing required inputs or type mismatches.
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

            # Normalize expected input types into a list of CWL types
            expected_types: list[str] = []
            if isinstance(input_dict["type"], str):
                expected_types = [input_dict["type"]]
            elif isinstance(input_dict["type"], list):
                expected_types = input_dict["type"]
            else:
                raise Exception(f"Unexpected type(input_dict['type']). Should be 'str' or 'list', but found '{type(input_dict['type'])}'")
            
            optional: bool = "null" in expected_types

            # Load value from runtime_context
            global_source_id: str = self.input_to_source[input_id]
            v = self.runtime_context[global_source_id]

            if isinstance(v, (NoneType, Absent)):
                if not optional:
                    raise Exception(f"Tool input '{input_id}' supports CWL types [{', '.join(expected_types)}], but found CWL type '{value_cwl_t}' (from '{type(value)}')")
                continue

            value = v.value
            value_cwl_t = v.cwltype
            is_array = v.is_array

            # Files and directories can come in the form of a simple string, so
            # we have to check for that.
            match: str | None = None
            if value_cwl_t == "string":
                if "file" in expected_types or "file[]" in expected_types:
                    match = "file"
                elif "directory" in expected_types or "directory[]" in expected_types:
                    match = "directory"
                elif "string" in expected_types or "string[]" in expected_types:
                    match = "string"
            elif value_cwl_t in expected_types or value_cwl_t + "[]" in expected_types:
                match = value_cwl_t

            if match is None:
                if not optional:
                    raise Exception(f"Tool input '{input_id}' supports CWL types [{', '.join(expected_types)}], but found CWL types '{value_cwl_t} (from '{type(value)}')")
                continue

            if v.type in (FileObject, DirectoryObject):
                if is_array:
                    resolved = []
                    for elem in value:
                        resolved.append(elem.resolve())
                    value = resolved
                else:
                    value = value.resolve()

            # expects_array: bool = value_cwl_t + "[]" in expected_types and is_array
            # Check whether an array is expected or not
            if value_cwl_t + "[]" in expected_types and is_array:
                # Compose argument with array data
                cmd.extend(self.compose_array_arg(value, value_cwl_t, input_dict))
            else:
                # Compose argument with single data value
                cmd.extend(self.compose_arg(value, value_cwl_t, input_dict))

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

        Args:
            cwl_namespace: Namespace used to evaluate EnvVarRequirement values
                (expressions may appear in those values).

        Returns:
            A dictionary representing the environment variables to use for
            subprocess execution. Keys include at minimum ``HOME``,
            ``TMPDIR`` and ``PATH`` and may be extended by ``EnvVarRequirement``.
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
            env: dict[str, Any],
            cwd: Path,
        ) -> dict[str, Value]:
        """Execute the command and produce CWL-typed outputs.

        This wrapper runs ``cmd`` via ``subprocess.run``, captures standard
        output and error, and then maps files and evaluated expressions to
        CWL outputs according to ``output_schema``. The function returns a
        dictionary of new runtime outputs keyed by global output id. The
        returned mapping includes the raw ``stdout`` and ``stderr`` under
        those keys.

        Args:
            cmd: The command token list to execute.
            cwl_namespace: Namespace used for evaluating output globs and
                expressions (``outputEval``).
            output_schema: Dictionary describing outputs and their bindings.
            env: Environment variables to pass to the subprocess.

        Returns:
            A dict mapping global output ids to ``Value`` instances (or
            ``None`` for missing outputs) and includes keys ``stdout`` and
            ``stderr`` containing captured bytes.

        Raises:
            NotImplementedError for features not yet implemented (e.g.
            ``loadContents`` handling), or re-raises subprocess errors.
        """
        # Execute tool
        print("[TOOL]: EXECUTING:", " ".join(cmd))
        try:
            stdin = sys.stdin
            stdout = sys.stdout
            stderr = sys.stderr
            if "stdin" in self.io:
                stdin = open(self.io["stdin"], "r")
            if "stdout" in self.io:
                stdout = open(self.io["stdout"], "w")
            if "stderr" in self.io:
                stderr = open(self.io["stderr"], "w")

            completed: CompletedProcess = run(
                cmd,
                stdin = stdin,
                stdout = stdout,
                stderr = stderr,
                env = env,
                cwd = cwd,
            )
        except Exception as e:
            raise e

        # Process outputs.
        # Command outputs are matched against the tool's output schema. Tool 
        # outputs are generated based on the output bindings defined in the
        # output schema.
        outputs: dict[str, Any] = {}
        for output_id, output_dict in output_schema.items():
            global_output_id = self.global_id(output_id)
            
            # Get and normalize types into lis of CWL types
            expected_types: List[str]
            if "type" not in output_dict:
                raise Exception(f"Output {output_id} is missing type")
            if isinstance(output_dict["type"], List):
                expected_types = output_dict["type"]
            elif isinstance(output_dict["type"], str):
                expected_types = [output_dict["type"]]
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
                
                # Validate that list of globs contains strings
                for g in globs:
                    if not isinstance(g, str):
                        raise Exception(f"Output '{output_id}' has glob '{g}' that should be 'str', but is '{type(g)}'")
                    
                # Collect matching files. Because not CWD but a tmp CWD is,
                # the tmp CWD path needs to be prefixed. 
                output_file_paths: List[str] = []
                for g in globs:
                    for match in glob.glob(g, root_dir = cwd):
                        match_path = Path(cwd) / Path(match)
                        output_file_paths.append(str(match_path))
                        
                # Set 'self' for outputEval and loadContents
                cwl_namespace["self"] = output_file_paths

                # Set glob matches as output value
                if len(output_file_paths) == 0:
                    outputs[global_output_id] = None
                elif "file" in expected_types or "stdout" in expected_types:
                    outputs[global_output_id] = Value(FileObject(output_file_paths[0]), FileObject, "file")
                elif "file[]" in expected_types:
                    outputs[global_output_id] = Value([FileObject(p) for p in output_file_paths], FileObject, "file")
                elif "directory" in expected_types:
                    outputs[global_output_id] = Value(DirectoryObject(output_file_paths[0]), DirectoryObject, "directory")
                elif "directory[]" in expected_types:
                    outputs[global_output_id] = Value([DirectoryObject(p) for p in output_file_paths], DirectoryObject, "directory")
                elif "string" in expected_types:
                    outputs[global_output_id] = Value(output_file_paths[0], str, "string")
                elif "string[]" in expected_types:
                    outputs[global_output_id] = Value(output_file_paths, str, "string")

            # loadContents
            if ("loadContents" in output_dict and
                output_dict["outputEval"] is not None):
                if not any(["file" in type_ for type_ in expected_types]):
                    raise Exception(f"loadContents must only be used on 'file' or 'file[]' type")
                # TODO
                raise NotImplementedError()

            # outputEval
            if ("outputEval" in output_dict and 
                output_dict["outputEval"] is not None):
                # Evaluated expression might yield any type, so validation is needed
                value = self.eval(output_dict["outputEval"], cwl_namespace)

                # Match received and expected CWL types of output
                is_array: bool = False
                if isinstance(value, List):
                    # Value is an array
                    is_array = True

                    # Empty arrays dont have item types, so any can be assigned
                    if len(value) == 0:
                        ... # TODO
                        
                 
                    # All array elements must have the same type.
                    if any([not isinstance(v, type(value[0])) for v in value]):
                        raise Exception("Array is not homogeneous")   
                    expected_types = [t for t in expected_types if "[]" in t] # Filter for array types
                    value_types = PY_CWL_T_MAPPING[type(value[0])]
                else:
                    # Dealing with a single item
                    value_types = PY_CWL_T_MAPPING[type(value)]

                # Check if the type of the received runtime input value is 
                # valid and return get the matching CWL type
                value_type: str | None = None
                for value_t in value_types:
                    for valid_type in expected_types:
                        if is_array:
                            if value_t + "[]" == valid_type:
                                value_type = value_t + "[]"
                        else:
                            if value_t == valid_type:    # Exact string match
                                value_type = value_t
                                break   # Stop searching if a match is found

                if value_type is None:
                    raise Exception(f"No matching type found for output {output_id}")

                # Wrap the validated result in a Value instance and save to outputs
                _type = CWL_PY_T_MAPPING[value_type.replace("[]", "")]
                if is_array:
                    outputs[global_output_id] = Value([_type(v) for v in value], _type, value_type)
                else:
                    outputs[global_output_id] = Value(_type(value), _type, value_type)

            # secondaryFiles
            if ("secondaryFiles" in output_dict and output_dict["outputEval"] is not None):
                if not any(["file" in type_ for type_ in expected_types]):
                    raise Exception(f"secondaryFiles must only be used on 'file' or 'file[]' type")
                # TODO Check if actual output is file or file[]
                raise NotImplementedError()
                # if isinstance(output[global_output_id], List): 
                #     output[global_output_id].extend(Value(...))
                # else:
                #     ???
            
        return outputs

        
    def execute(
            self, 
            use_dask: bool,
            runtime_context: Optional[dict[str, Any]] = None,
            verbose: Optional[bool] = True,
            client: Optional[Client] = None,
        ) -> dict[str, Value]:
        """Top-level execution entrypoint for the tool.

        This method prepares the runtime environment, builds the
        command line, and executes the tool either locally
        or via a Dask ``Client``. Outputs are returned in a dict with the
        output ID as key.

        Args:
            use_dask: If True, submit the work to the provided or a new
                Dask client; otherwise run locally.
            runtime_context: Optional mapping of runtime inputs to override
                the instance's current ``runtime_context``.
            verbose: Whether to print progress and debugging information.
            client: Optional Dask ``Client`` to use when ``use_dask`` is
                True.

        Returns:
            A dictionary mapping global output ids to ``Value`` objects.
        """
        # Update runtime context
        if runtime_context is not None:
            self.runtime_context = runtime_context

        # Create a temporary work directory and stage all needed files
        tmp_path: Path = self.loading_context["designated_tmp_dir"]
        tmp_path /= str(uuid4())
        tmp_path.mkdir()
        self.stage_files(tmp_path)

        # Build the command line
        cwl_namespace = self.build_namespace()
        self.prepare_runtime_context(cwl_namespace)
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
                cwd = tmp_path,
                pure = False
            )
            new_state = future.result()
        else:
            new_state = self.run_wrapper(
                cmd,
                cwl_namespace,
                self.outputs,
                env,
                cwd = tmp_path,
            )
        return new_state