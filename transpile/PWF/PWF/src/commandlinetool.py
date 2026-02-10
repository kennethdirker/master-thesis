
import glob
import json
import os
import shutil
import sys

from abc import abstractmethod
from dask.distributed import Client
from pathlib import Path
from subprocess import run, CompletedProcess
from uuid import uuid4

from types import NoneType
from typing import (
    Any,
    cast,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from .process import BaseProcess
from .utils import (
    Absent,
    FileObject,
    DirectoryObject,
    Value,
    PY_CWL_T_MAPPING,
    CWL_PY_T_MAPPING
)


class BaseCommandLineTool(BaseProcess):
    # Tool info
    io: Dict[str, Any]
    base_command: List[str] | str | None
    arguments: List[Union[str, Dict[str, Union[str, int]]]]

    def __init__(
            self,
            main: bool = True,
            loading_context: Optional[Dict[str, str]] = None,
            parent_process_id: Optional[str] = None,
            step_id: Optional[str] = None,
            inherited_requirements: Optional[Dict[str, Any]] = None,
            PATH: Optional[str] = None,
        ):
        """ TODO: class description """

        # Initialize BaseProcess class
        super().__init__(
            main = main,
            loading_context = loading_context,
            parent_process_id = parent_process_id,
            step_id = step_id,
        )

        # Prepare properties
        self.base_command = None
        if PATH is None:
            PATH = os.environ.get("PATH", "")

        # Digest CommandlineTool file
        self.set_base_command()
        self.set_arguments()
        self.set_io()
        self.process_requirements(inherited_requirements)
        
        if main:
            yaml_uri = self.loading_context["input_object"]
            runtime_context = self._load_input_object(yaml_uri)
            self.register_input_sources()
            outputs = self.execute(self.loading_context["use_dask"],
                                   runtime_context)
            self.finalize(outputs)


    def set_metadata(self):
        """
        Assign metadata to the process.

        Can be overwritten by user to assign the following variables:
            - self.label: Human readable short description.
            - self.doc: Human readable process explaination.
            - TODO: Add more fields if needed!
        """
        self.metadata = {}

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

    def set_arguments(self) -> None:
        """
        TODO
        """
        self.arguments = []


    def set_io(self) -> None:
        """
        Set the tool's IO options.
        """
        self.io = {}


    def process_requirements(
            self,
            inherited_requirements: Dict[str, Any] | None
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

        # $include tokens need to be replaced with the contents of the
        # file pointed to by its url.
        if "InlineJavascriptRequirement" in self.requirements:
                js_req = self.requirements["InlineJavascriptrequirement"]
                for i, chunk in enumerate(js_req.copy()):
                    if "$include" in chunk:
                        include = chunk.strip().removeprefix("$include").strip()
                        js_req[i] = self.inline_include(include)

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


    def stage_initial_work_dir_requirement(
            self, 
            tmp_path: Path,
            cwl_namespace: Dict[str, Any]
        ) -> None:
        # Stage files from InitialWorkDirRequirement
        if not "InitialWorkDirRequirement" in self.requirements:
            return
        
        requirement = self.requirements["InitialWorkDirRequirement"]
        # Normalize if expression
        if isinstance(requirement, str):
            requirement = [requirement]
        
        for listing in requirement:
            if isinstance(listing, str):
                # Listing is expression
                ret = self.eval(listing, cwl_namespace)
                
                # Normalize
                if not isinstance(ret, List):
                    ret = [ret]
                if isinstance(ret, NoneType) or len(ret) == 0: 
                    continue
                
                # Copy Files to working directory
                for x in ret:
                    if isinstance(x, str):
                        # NOTE Path?
                        path = Path(x)
                    elif isinstance(x, (FileObject, DirectoryObject)):
                        path = Path(x.path)
                    else:
                        raise NotImplementedError(x, type(x))
                    
                    if path.is_file():
                        shutil.copy(path, tmp_path)
                    elif path.is_dir():
                        shutil.copytree(path, tmp_path)

            elif isinstance(listing, dict):
                if "entry" in listing:
                    # Dirent
                    # Evaluate 'entry' field
                    entry = listing["entry"]
                    contents: str | None = None
                    is_file: bool = False
                    is_dir: bool = False
                    if isinstance(entry, NoneType): continue
                    if isinstance(entry, str): 
                        ret = self.eval(line, cwl_namespace)
                        if isinstance(ret, str):
                            is_file = True
                            contents = ret
                        elif isinstance(ret, FileObject):
                            is_file = True
                            raise NotImplementedError()
                        elif isinstance(ret, DirectoryObject):
                            is_dir = True
                            raise NotImplementedError()
                        else:
                            is_file = True
                            contents = json.dumps(ret, indent = 4)
                    elif isinstance(entry, List):
                        # Multiline, each needs evaluation and then form file.contents together
                        is_file = True
                        eval_lines = []
                        for line in entry:
                            eval_lines.append(self.eval(line, cwl_namespace))
                        contents = "\n".join(eval_lines)
                    else:
                        raise Exception(entry, type(entry))

                    # Evaluate "entryname" field
                    if "entryname" in listing:
                        entryname = self.eval(listing["entryname"], cwl_namespace)
                        if isinstance(entryname, str):
                            # TODO
                            # Must be relative path within tmp CWD
                            entry_path = tmp_path / Path(entryname)
                            entry_path = entry_path.resolve()
                            if not entry_path.is_relative_to(tmp_path):
                                raise Exception("Must create file/directory within working folder")
                    
                    # Create files/directories
                    if is_file:
                        FileObject(entry_path).create(contents)
                    elif is_dir:
                        DirectoryObject(entry_path).create()
                    else:
                        raise Exception("Listing is neither a file or a directory")
                else:
                    raise NotImplementedError(listing)
            else:
                raise NotImplementedError(listing)


    def stage_input_files(
            self, 
            tmp_path: Path,
            runtime_context: Dict[str, Any]) -> None:
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
            value_wrapper = runtime_context[source]
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
            runtime_context[source] = new_value


    def stage_files(
            self, 
            tmp_path: Path,
            cwl_namespace: Dict[str, Any],
            runtime_context: Dict[str, Any]
        ) -> None:
        """
        TODO Description
        """
        self.stage_initial_work_dir_requirement(tmp_path, cwl_namespace)
        self.stage_input_files(tmp_path, runtime_context)


    def prepare_arguments(
            self, 
            cwl_namespace: Dict[str, Any]
        ) -> List[Dict[str, Any]]:
        """
        TODO Description
        """
        processed = []
        # Normalize to inputBinding dicts with default fields
        for arg in self.arguments:
            if isinstance(arg, str):
                value = self.eval(arg, cwl_namespace)
                if value is None:
                    continue

                d = {
                    "type": "argument",
                    "valueFrom": value,
                    "position": 0,
                    "separate": True,
                }
                processed.append(d)
            elif isinstance(arg, Dict):
                value = self.eval(cast(str, arg["valueFrom"]), cwl_namespace)
                if value is None:
                    continue

                arg["type"] = "argument"
                arg["valueFrom"] = value
                if not "position" in arg:
                    arg["position"]
                if not "separate" in arg:
                    arg["separate"] = True
                processed.append(arg)
            else:
                raise Exception(f"Expected 'str' or 'dict', but found '{type(arg)}'")
        
        # Compose the argument and save the individual parts as a list of 
        # strings in the valueFrom field.
        for arg in processed:
            value = arg["valueFrom"]
            if isinstance(value, List):
                if len(value) == 0: 
                    continue

                cwl_value_t = PY_CWL_T_MAPPING[type(value[0])][0]
                arg["valueFrom"] = self.compose_array_arg(value, cwl_value_t, arg)
            else:
                cwl_value_t = PY_CWL_T_MAPPING[type(value)][0]
                arg["valueFrom"] = self.compose_arg(value, cwl_value_t, arg)
        return processed



    def prepare_runtime_context(
            self,
            cwl_namespace: Dict[str, Any],
            runtime_context: Dict[str, Any]
        ) -> None:
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
            value = runtime_context[global_source_id].value

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
            namespace: Dict[str, Any] = cwl_namespace.copy()
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
            runtime_context[global_source_id] =  Value(value, value_t, PY_CWL_T_MAPPING[value_t][0])


    def compose_array_arg(
            self,
            values: Any | List[Any],
            cwl_value_t: str,
            input_dict: Dict[str, Any]
        ) -> List[str]:
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

        array_arg: List[str] = []

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
            input_dict: Dict[str, Any],
        ) -> List[str]:
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

        args: List[str] = []

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


    def build_commandline(
            self,
            arguments: List[Dict[str, Any]],
            runtime_context: Dict[str, Any]
        ) -> List[str]:
        """Construct the full command line for the tool from runtime input values.

        The method iterates over the tool's inputs, orders positional
        parameters by their ``position``, validates runtime types against the
        declared CWL types, and delegates formatting to
        ``compose_array_arg`` or ``compose_arg``. If ``self.base_command`` is
        set it is prepended to the generated argument list.

        Precondition:
            ``prepare_runtime_context()`` must have been called so that
            ``runtime_context`` contains evaluated ``Value`` instances.

        Returns:
            A list of string tokens representing the full command to be
            executed.

        Raises:
            Exception on missing required inputs or type mismatches.
        """
        cmd: List[str] = []

        # [(input_id, input_dict), ...]
        pos_inputs: List[Tuple[str, Dict[str, Any]]] = []
        key_inputs: List[Tuple[str, Dict[str, Any]]] = []

        # Arguments with position 0 should be inserted before inputs with
        # position 0. We filter arguments with pos 0 so they can be prepended
        # later on.
        for arg in arguments:
            if arg["position"] == 0:
                cmd.extend(arg["valueFrom"])
            else:
                pos_inputs.append(("", arg))

        # Separate positional inputs from key inputs
        for input_id, input_dict in self.inputs.items():
            # Skip unbound (without input binding) inputs
            if "bound" in input_dict and input_dict["bound"]:
                if hasattr(input_dict, "position"):
                    pos_inputs.append((input_id, input_dict))
                else:
                    key_inputs.append((input_id, input_dict))

        # Order the inputs
        inputs: List[Tuple[str, Dict]] = sorted(pos_inputs, key=lambda x: x[1]["position"])
        inputs += key_inputs

        # Match inputs with runtime input values, evaluate positional arguments
        # and add them to the command-line
        for input_id, input_dict  in inputs:
            # Handle positionals from arguments field
            if "argument" in input_dict["type"]:
                cmd.extend(input_dict["valueFrom"])
                continue

            # Normalize expected input types into a list of CWL types
            expected_types: List[str] = []
            if isinstance(input_dict["type"], str):
                expected_types = [input_dict["type"]]
            elif isinstance(input_dict["type"], List):
                expected_types = input_dict["type"]
            else:
                raise Exception(f"Unexpected type(input_dict['type']). Should be 'str' or 'list', but found '{type(input_dict['type'])}'")
            
            optional: bool = "null" in expected_types

            # Load value from runtime_context
            global_source_id: str = self.input_to_source[input_id]
            v = runtime_context[global_source_id]

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

            # Check whether an array is expected or not
            if value_cwl_t + "[]" in expected_types and is_array:
                # Compose argument with array data
                cmd.extend(self.compose_array_arg(value, value_cwl_t, input_dict))
            else:
                # Compose argument with single data value
                cmd.extend(self.compose_arg(value, value_cwl_t, input_dict))

        # Combine the base command with the arguments
        if hasattr(self, "base_command"):
            if isinstance(self.base_command, List):
                cmd = [*self.base_command] + cmd
            elif isinstance(self.base_command, str):
                cmd = [self.base_command] + cmd
            else:
                raise Exception(f"base_command should be 'str' or 'list[str]',"
                                f"but found '{type(self.base_command)}'")
        return cmd
    

    def build_env(self, cwl_namespace) -> Dict[str, Any]:
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
        env: Dict[str, Any] = {
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
            cmd: List[str],
            cwl_namespace: Dict[str, Any],
            output_schema: Dict[str, Any],
            env: Dict[str, Any],
            cwd: Path,
        ) -> Dict[str, Value]:
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
        print("[TOOL]: EXECUTING:", " \\\n[TOOL]:\t\t".join(cmd))
        try:
            stdin = sys.stdin
            stdout = sys.stdout
            stderr = sys.stderr
            if "stdin" in self.io:
                stdin = open(cwd / self.io["stdin"], "r")
            if "stdout" in self.io:
                stdout = open(cwd / self.io["stdout"], "w")
            if "stderr" in self.io:
                stderr = open(cwd / self.io["stderr"], "w")

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
        outputs: Dict[str, Any] = {}
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
            runtime_context: Dict[str, Any],
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
        cwl_namespace = self.build_base_namespace(runtime_context)

        # Create a temporary work directory and stage all needed files
        tmp_path: Path = self.loading_context["designated_tmp_dir"]
        tmp_path /= str(uuid4())
        tmp_path.mkdir()
        self.stage_files(tmp_path, cwl_namespace, runtime_context)

        # Build the command line
        self.prepare_runtime_context(cwl_namespace, runtime_context)
        arguments = self.prepare_arguments(cwl_namespace)
        cmd: List[str] = self.build_commandline(arguments, runtime_context)

        # Build the execution environment
        env = self.build_env(cwl_namespace)

        # Submit and execute tool and gather output
        new_state: Dict[str, Value] = {}
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