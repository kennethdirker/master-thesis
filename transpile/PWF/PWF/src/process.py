import argparse
import inspect
import js2py
import json
import os
import shutil
import uuid
import yaml

from abc import ABC, abstractmethod
from copy import deepcopy
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Type,
    Union,
    cast
)

from .utils import (
    Absent,
    DirectoryObject,
    FileObject,
    CWL_PY_T_MAPPING,
    PY_CWL_T_MAPPING,
    Value,
    dict_to_obj
)


class BaseProcess(ABC):
    # Process info
    is_main: bool
    short_id: str
    process_path: str
    id: str

    # Parent process info
    parent_process_id: str | None
    step_id: str | None

    # Tool/workflow info
    metadata: Dict[str, Any]
    inputs: Dict[str, Any]
    outputs: Dict[str, Any]
    requirements: Dict[str, Any]

    # Runtime info
    input_to_source: Dict[str, str]
    loading_context: Dict[str, Any]
    runtime_context: Dict[str, Value | Absent]

    def __init__(
            self,
            main: bool = True,
            loading_context: Optional[Dict[str, Any]] = None,
            parent_process_id: Optional[str] = None,
            step_id: Optional[str] = None,
            inherited_requirements: Optional[Dict[str, Any]] = None,
        ) -> None:
        """ 
        TODO: class description. Which vars are accessable?

        Arguments:
        main
        # TODO Explain runtime_context
        # TODO Explain loading_context
            - processes (list[Process])
            - input_object dict[str, Any]
            - PATH (str)
            - init_work_dir (Path)
            - designated_out_dir (Path)
            - init_out_dir_empty (bool)
            - designated_tmp_dir (Path)
            - init_tmp_dir_empty (bool)
            - preserve_tmp (bool)
            - use_dask  (bool)

        Implementation NOTE: BaseProcess __init__ must be called from the
        subclass __init__ before any other state is touched.
        """
        # A process that is called from the command-line is root. Processes
        # that are instantiated from other another process (sub-processes)
        # are not root.
        self.is_main = main

        # Unique identity of a process instance. The id looks as follows:
        #   "{path/to/process/script/file}:{uuid}"  
        # FIXME: IDs could be made of {uuid} only, but the path adds debugging clarity.
        self.short_id = str(uuid.uuid4())
        self.process_path = str(Path(inspect.getfile(type(self))).resolve())
        self.id = self.process_path + ":" + self.short_id

        # ID of the step and process from which this process is called.
        # Both are None if this process is the root process.
        if main:
            step_id = None
            parent_process_id = None
        self.parent_process_id = parent_process_id
        self.step_id = step_id

        # Assign metadata attributes. Override in self.set_metadata().
        self.metadata = {}
        
        # Assign input/output dictionary attributes.
        # FIXME: dicts could use custom types like CWLTool does, instead of dicts.
        self.inputs = {} # Override in set_inputs()
        self.outputs = {} # Override in set_outputs()

        # Maps input_id to its global source id, which is used as key in runtime_context
        self.input_to_source =  {}  # {input_id, global_source_id}
        
        # Assign requirements and hints.
        # Override in set_requirements().
        self.requirements = {}

        # NOTE: Not sure if I want to support hints, force requirements only?
        # self.hints: Dict = {}   
        
        # Digest basic process attributes
        self.set_metadata()
        self.set_inputs()
        self.set_outputs()
        self.set_requirements()
        
        # TODO Update description
        # Assign a dictionary with runtime input variables and a dictionary to
        # map processes and step IDs. Only the root process must load the input
        # YAML from a file. Non-root processes get a reference to the
        # dictionary loaded in the main process.
        if main:
            self.loading_context = {}
            self.loading_context["processes"] = {}  # {proc_id, process}
            self.process_cli_args(self.loading_context)

            # Copy system PATH environment variable
            self.loading_context["PATH"] = os.environ.get("PATH")

            # Save the path to the initial current work directory
            self.loading_context["init_work_dir"] = Path(os.getcwd())
        else:
            if loading_context is None:
                raise Exception(f"Subprocess {type(self)}({self.id}) is not initialized as root process, but lacks loading context")
            self.loading_context = loading_context

        # Register this process in the global cache
        self.loading_context["processes"][self.id] = self


    def create_parser(self) -> argparse.ArgumentParser:
        """
        Create and return an argument parser for command-line arguments.

        TODO Description (schema) of arguments
        python PWF.py [--outdir OUTDIR, --tmpdir TMPDIR, --preserve_tmp, --dask] input_object.yaml
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-y', "--yaml", 
            type = str,
            help='Path to input object YAML file.', 
            required=True
        )
        parser.add_argument(
            "--outdir",
            default = os.getcwd(),
            type = str,
            help = "Directory to store output files in."
        )
        parser.add_argument(
            "--tmpdir",
            type = str,
            default = "/tmp/" + str(uuid.uuid4()),
            help="Directory to store temporary files in."
        )
        parser.add_argument(
            "--preserve_tmp",
            action="store_true",
            default = False,
            help="Preserve temporary files and directories created for tool execution instead of deleting them."
        )
        parser.add_argument(
            "--dask",
            action="store_true",
            default=False,
            help="Execute process tasks with Dask instead of with standard system call."
        )
        return parser
    
    
    def process_cli_args(self, loading_context: Dict[str, Any]) -> None:
        """
        Process CLI arguments.
        TODO Description of arguments
        TODO out dir(empty), tmp dir(empty), use dask
        """
        parser = self.create_parser()
        args = parser.parse_args()

        # Check input object validity
        input_object_path = Path(args.yaml).absolute()
        if not input_object_path.is_file():
            raise Exception(f"Input object file {args.input_object} does not exist or is not a file")
        loading_context["input_object"] = input_object_path 
        print(f"[PROCESS]: Input object file:")
        print(f"[PROCESS]:\t{input_object_path}")

        # Configure designated output directory
        out_dir_path = Path(args.outdir).absolute()
        if out_dir_path.exists() and not out_dir_path.is_dir():
            raise Exception(f"Output directory {out_dir_path} is not a directory")
        out_dir_path.mkdir(parents=True, exist_ok=True)            # Create out dir
        is_empty = not any(out_dir_path.iterdir())  # Check if out dir is empty
        loading_context["init_out_dir_empty"] = is_empty
        loading_context["designated_out_dir"] = out_dir_path
        print(f"[PROCESS]: Designated output directory:")
        print(f"[PROCESS]:\t{loading_context['designated_out_dir']}")

        # Configure designated temporary directory
        tmp_dir_path = Path(args.tmpdir).absolute()
        if tmp_dir_path.exists() and not tmp_dir_path.is_dir():
            raise Exception(f"Temporary directory {tmp_dir_path} is not a directory")
        tmp_dir_path.mkdir(parents=True, exist_ok=True)            # Create tmp dir
        is_empty = not any(tmp_dir_path.iterdir())  # Check if tmp dir is empty
        loading_context["init_tmp_dir_empty"] = is_empty
        loading_context["designated_tmp_dir"] = tmp_dir_path
        loading_context["preserve_tmp"] = args.preserve_tmp
        s = "" if args.preserve_tmp else "not "
        print(f"[Process]: Designated temporary directory ({s}preserved):")
        print(f"[PROCESS]:\t{tmp_dir_path}")

        # Configure whether Dask is used for execution
        loading_context["use_dask"] = args.dask
        print(f"[PROCESS]: Execute with Dask: {args.dask}")


    def global_id(self, s: str) -> str:
        """
        Concatenate the process UID and another string, split by a colon.
        """
        return self.id  + ":" + s


    def _load_yaml(self, yaml_uri: str) -> Dict[str, Any]:
        """
        Load a YAML file pointed at by 'yaml_uri' into a dictionary.
        """
        # NOTE: BaseLoader is used to force the YAML reader to only create
        # string objects, instead of interpreting and converting objects to
        # Python objects. This is needed for cases like booleans, where
        # SafeLoader creates a boolean True when reading true and True. This
        # behaviour is not always wanted.
        with open(Path(yaml_uri), "r") as f:
            y = yaml.load(f, Loader=yaml.BaseLoader)
            if not isinstance(y, Dict):
                raise Exception(f"Loaded YAML should be a dict, but has type {type(y)}")
            return y
        
    
    def resolve_input_object_value(
            self, 
            input_id: str,
            input_value: Any
        ) -> Value:
        """
        Extract a value from a input object's key-value entry. This is needed
        because CWL input objects may contain key-value pairs that are more
        complicated than needed... Below is an example where we need to 
        extract the path.

        Examples of special cases:
        input_file:
            class: File
            path: path/to/file

        input_files: 
            - {class: File, path: path/to/file1} 
            - {class: File, path: path/to/file2}

        Arguments:
            input_value: An object value from the primary key-value YAML layer.

        Returns:
            The extracted value.
        """
        expected_cwl_types: List[str]
        if not isinstance(self.inputs[input_id]["type"], List):
            expected_cwl_types = [self.inputs[input_id]["type"]]
        else:
            expected_cwl_types = self.inputs[input_id]["type"]


        if isinstance(input_value, List):
            # We are dealing with an array
            if len(input_value) == 0:
                # Empty arrays dont add to command line, so just put None
                return Value(None, type(None), "null")
            
            # Get the first array item
            head = input_value[0]

            # Check if all array elements have the same type.
            if any([not isinstance(v, type(head)) for v in input_value]):
                raise Exception(f"Input {input_id} is non-homogeneous array")
            
            # Filter for array types and remove [] from cwl type
            expected_cwl_types = [t[:-2] for t in expected_cwl_types if "[]" in t]

            if isinstance(head, str):
                # Files and directories can come in the form of a string path,
                # which we need to check for.
                # Match input to schema
                value_types = PY_CWL_T_MAPPING[type(head)]
                matched_types = [t for t in value_types if t in expected_cwl_types]
                if len(matched_types) == 0:
                    raise Exception(f"Input '{input_id}' did not match the input schema")
            
                if "file" in matched_types:
                    return Value([FileObject(p) for p in input_value], FileObject, "file")
                if "directory" in matched_types:
                    return Value([DirectoryObject(p) for p in input_value], DirectoryObject, "directory")
            
            if isinstance(head, Mapping):
            # A mapping either means a potential file/directory, or an
            # unsupported custom data type. Unsupported types result in error.
                if "class" in head:
                    if "File" in head["class"]:
                        return Value(
                            [FileObject(p["path"]) for p in input_value], 
                            FileObject, 
                            "file"
                        )
                    elif "Directory" in head["class"]:
                        return Value(
                            [DirectoryObject(p["path"]) for p in input_value],
                            DirectoryObject, 
                            "directory"
                        )
                    else:
                        raise Exception(f'Found unsupported class in {input_id}: {input_value[0]["class"]}')

            return Value(
                input_value, 
                type(input_value[0]), 
                PY_CWL_T_MAPPING[type(head)][0] # Mapping isnt 1-to-1, so take first item
            )
        elif isinstance(input_value, Mapping):
            # A mapping either means a potential file/directory, or an
            # unsupported custom data type. Unsupported types result in error.
            if "class" in input_value:
                if "File" in input_value["class"]:
                    return Value(
                        FileObject(input_value["path"]), 
                        FileObject, 
                        "file"
                    )
                elif "Directory" in input_value["class"]:
                    return Value(
                        DirectoryObject(input_value["path"]), 
                        DirectoryObject, 
                        "directory"
                    )
                else:
                    raise Exception(f'Found unsupported class in {input_id}: {input_value["class"]}')
            else:
                raise NotImplementedError()
        else:
            if type(input_value) not in PY_CWL_T_MAPPING:
                raise Exception(f"Found unsupported Python value type {type(input_value)} for input {input_id}")

            # Match input to schema
            value_types = PY_CWL_T_MAPPING[type(input_value)]
            matched_types = [t for t in value_types if t in expected_cwl_types]
            if len(matched_types) == 0:
                raise Exception(f"Input '{input_id}' did not match the input schema")

            if isinstance(input_value, str):
                # It is valid in CWL to pass files and directories as a simple
                # string path, which we need to check for.
                if "file" in matched_types:
                    return Value(
                        FileObject(input_value),
                        FileObject,
                        "file"
                    )
                elif "directory" in matched_types:
                    return Value(
                        DirectoryObject(input_value),
                        DirectoryObject,
                        "directory"
                    )

            return Value(
                input_value,
                type(input_value),
                PY_CWL_T_MAPPING[type(input_value)][0]
            )

    
    def _load_input_object(self, yaml_uri: str) -> Dict[str, Any]:
        """
        Read the input object from a YAML file and map the values
        with the globalized input ID as key.

        Arguments:
            yaml_uri: Path to the input object YAML file.
        
        Returns a dictionary that maps global process input IDs to values from
        the input object
        """
        runtime_context = {}
        input_obj = self._load_yaml(yaml_uri)

        print("[PROCESS]: Inputs loaded into runtime context:")
        for input_id, input_value in input_obj.items():
            # Input from object is indexed by '{Process.id}:{input_id}'
            input_value = self.resolve_input_object_value(input_id, input_value)
            runtime_context[self.global_id(input_id)] = input_value
            print("[PROCESS]: \t-", input_id, ":", input_value)
        print()

        return runtime_context


    # @abstractmethod
    def set_metadata(self) -> None:
        """
        TODO Better description
        Must be overridden to assign process metadata attributes.
        """
        # pass
        self.metadata = {}


    # @abstractmethod
    def set_inputs(self) -> None:
        """ 
        TODO Better description
        This function must be overridden to define input job order field 
        requirements. These job order requirements are used to test whether a 
        tool is ready to be executed.
        """
        # Example:
        # self.inputs = {
        #     "url_list": {
        #         "type": "file"
        #     }
        # }
        pass
    
    
    @abstractmethod
    def set_outputs(self) -> None:
        """
        TODO Better description
        This function must be overridden to define Process outputs.
        """
        # Example:
        # self.outputs = {
        #     "before_noise_remover": {
        #         "type": "file",
        #         # "outputSource": {input_arg_id}
        #         # "outputSource": {step_id}/{step_output_id}
        #         "outputSource": "imageplotter/output"
        #     }
        # }
        pass


    # @abstractmethod
    def set_requirements(self) -> None:
        """
        TODO Better description
        This function can be overridden to indicate execution requirements. As
        requirements are essentially optional, it is not requried to override
        this method.
        """
        self.requirements = {}
        # Example:
        # 
        # 
        # 
        # pass


    @abstractmethod
    def register_input_sources(self) -> None:
        """
        Cache the source of each input with an ID unique to each Process 
        instance. The mapping is saved under self.input_to_sources. The key of
        each input can be found by using self.global_id().
        """
        pass


    def process_requirements(
            self,
            requirements: Optional[Dict[str, Any]] = None,
        ) -> None:
        """
        TODO Finish
        Combine inherited requirements with the process' own requirements.
        Processes can inherit certain requirements from their parents.
        Child processes can overwrite these inherited requirements, but
        in some cases inherited requirements should be added to, instead of
        completely replaced.
        """
        if requirements is None:
            return
        
        final_reqs: Dict[str, Any] = {}
        #  TODO Which requirements can be inherited?
        #     - InitialWorkDirRequirement
        #     -
        #     -
        #     -
        #     -
        self.requirements = final_reqs


    def build_base_namespace(
            self,
            runtime_context: Dict[str, Any],
            namespace: Optional[Dict[str, Any]] = None,
        ) -> Dict[str, Any]:
        """
        Create a dict that holds the CWL 'inputs' namespace. The namespace
        defines object name-value pairs that are used in the eval() call.
        Example: If the process has an input 'input_fits', it can be
                 accessed in the expression as 'inputs.input_fits'.
        TODO Other CWL namespaces, like 'runtime'?
        """
        if namespace is None:
            namespace = {}

        namespace["inputs"] = {}
        for input_id in self.inputs:
            source = self.input_to_source[input_id]
            value = runtime_context[source].value
            namespace["inputs"][input_id] = value

        return namespace


    @abstractmethod
    def execute(self):
        """
        TODO Better description
        """

    def __call__(self):
        """
        TODO
        """
        self.execute()


    def eval(
            self, 
            expression: str,
            local_vars: dict[str, Any],
            verbose: bool = False
        ) -> Any:
        """
        Evaluate an expression. Expressions are strings that come in 3 forms:
            1. String literal: "some string". Not evaluated.
            2. Javascript expression: "$( JS expression )". Evaluated with JS engine.
            3. Python expression: "$ Python expression $". Evaluated with Python eval().
        
        The expression may access CWL namespace variables, like 'inputs' and 'self'.
        """
        # TODO FIXME remove
        # verbose = True

        context_vars = local_vars.copy()

        # 'self' is null by default.
        if 'self' not in local_vars:
            context_vars.update({"self": None})

        if type(expression) is not str:
            raise Exception(f"Expected expression to be a string, but found {type(expression)}")
        
        # Evaluate expression. Evaluating can return any type     
        if expression.startswith("$(") and expression.endswith(")"):
            # Expression is a Javascript expression. Build JS context with CWL
            # namespaces and evaluate with JS engine.
            js_context = js2py.EvalJs(context_vars)

            # InlineJavascriptRequirement may contain JS code that must be
            # executed before the expressions evaluated. This JS code can then
            # be referenced in JS expressions.
            if "InlineJavascriptRequirement" in self.requirements:
                js_req = self.requirements["InlineJavascriptRequirement"]
                for chunk in js_req:
                    js_context.execute(chunk)

            try:
                ret = js_context.eval(expression[2:-1])
            except Exception as e:
                print(e)
                raise Exception(e.args, expression, local_vars)
            if isinstance(ret, js2py.base.JsObjectWrapper):
                # NOTE: https://github.com/PiotrDabkowski/Js2Py/blob/master/js2py/base.py#L1248
                # See link on how to check for array type.
                # TODO Support more types, like dict. How to handle custom
                # types?
                if ret._obj.Class in ('Array', 'Int8Array', 'Uint8Array', # type: ignore
                                 'Uint8ClampedArray', 'Int16Array',
                                 'Uint16Array', 'Int32Array', 'Uint32Array',
                                 'Float32Array', 'Float64Array', 'Arguments'):
                    ret = ret.to_list()
        elif expression.startswith("$") and expression.endswith("$"):
            # Expression is a Python expression. Build context dictionary with
            # CWL namespaces and evaluate expression with Python eval().
            py_context = context_vars.copy()
            py_context["inputs"] = dict_to_obj(context_vars["inputs"])
            ret = eval(expression[1:-1], py_context)
        else:
            # Expression is a plain string and doesn't need evaluating
            ret = expression
        if verbose: 
            print(f"[EVAL]: '{expression}' ({type(expression)}) -> '{ret}' ({type(ret)})")

        return ret
    

    def inline_include(self, file_url: str):
        """
        BUG FIXME
        Inherited InlineJavascriptRequirement $include probably points to
        files relatively to the parent that provides the requirement.

        TODO
        Every field of every document needs to be checked for $include
        statements...

        Return a string containing the contents of the file.
        """
        # BUG FIXME
        # Inherited InlineJavascriptRequirement $include probably points to
        # files relatively to the parent that provides the requirement.
        path = Path(file_url).resolve()
        if not path.is_file():
            raise Exception(f"{path} cant be included because it does not exist")

        with open(file_url, "r") as f:
            return f.read()


    def publish_output(
            self, 
            outputs: Dict[str, Any],
            verbose: bool = False
        ) -> str:
        """
        Copy the output files to the designated output directory and return a
        JSON string containing the outputs.
        """
        copy_alert = True
        new_outputs: Dict[str, Any] = {}
        for output_id in self.outputs:
            value_wrapper = outputs[self.global_id(output_id)]
            type_ = value_wrapper.type
            if type_ in (FileObject, DirectoryObject):
                # Files and directories need to be moved to the output directory
                if copy_alert and verbose:
                    print("[PROCESS]: Moving output to designated output directory:")
                    copy_alert = False

                values: List[Any]
                if value_wrapper.is_array:
                    values = value_wrapper.value
                else:
                    values = [value_wrapper.value]

                # Move the outputs to the output directory
                new_paths: List[str] = []
                for path_object in values:
                    old_path = Path(path_object.path)
                    out_dir = self.loading_context["designated_out_dir"]
                    new_path = shutil.move(old_path, out_dir)
                    new_paths.append(str(new_path))
                    if verbose:
                        print(f"[PROCESS]:\t- {output_id}: {old_path} >> {new_path}")

                new_outputs[output_id] = new_paths
                if not value_wrapper.is_array:
                    new_outputs[output_id] = new_paths[0]
            else:
                new_outputs[output_id] = value_wrapper.value
        return json.dumps(new_outputs, indent = 4)


    def delete_temps(self, verbose: bool = False):
        if self.loading_context["preserve_tmp"]: return
        if not self.loading_context["init_tmp_dir_empty"]:
            # Designated temporary directory was not empty on start, so we 
            # cannot savely delete the directory without deleting files from
            # other sources.
            print("[PROCESS]: Skip deleting temporary directory as it was initially not empty")
            return
        
        if verbose:
            print("[PROCESS]: Deleting temporary directory:")
            print(f"[PROCESS]:\t{self.loading_context['designated_tmp_dir']}")

        shutil.rmtree(self.loading_context["designated_tmp_dir"])


    def finalize(
            self, 
            outputs: Dict[str, Any], 
            verbose: bool = False
        ):
        print()
        print("[OUTPUT]")
        outputs_json = self.publish_output(outputs, verbose)
        self.delete_temps(verbose)
        print(outputs_json)