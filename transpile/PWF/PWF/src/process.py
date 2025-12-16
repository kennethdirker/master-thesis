import argparse
import copy
import inspect
import js2py
import os
import sys
import uuid
import yaml

from abc import ABC, abstractmethod
from copy import deepcopy
# from dask.delayed import Delayed
from dask.distributed import Client
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence, Optional, Type, Union, cast

from .utils import (
    Absent,
    DirectoryObject,
    FileObject,
    CWL_PYTHON_T_MAPPING,
    PYTHON_CWL_T_MAPPING,
    Value,
    dict_to_obj
)


class BaseProcess(ABC):
    def __init__(
            self,
            main: bool = True,
            runtime_context: Optional[dict[str, Any]] = None,
            loading_context: Optional[dict[str, Any]] = None,
            parent_process_id: Optional[str] = None,
            step_id: Optional[str] = None
        ) -> None:
        """ 
        TODO: class description. Which vars are accessable?

        Arguments:
        main
        # TODO Explain runtime_context
        # TODO Explain loading_context

        Implementation NOTE: BaseProcess __init__ must be called from the
        subclass __init__ before any other state is touched.
        """
        # A process that is called from the command-line is root. Processes
        # that are instantiated from other another process (sub-processes)
        # are not root.
        self.is_main: bool = main

        # Unique identity of a process instance. The id looks as follows:
        #   "{path/to/process/script/file}:{uuid}"  
        # FIXME: IDs could be made of {uuid} only, but the path adds debugging clarity.
        self.short_id: str = str(uuid.uuid4())
        self.process_path: str = str(Path(inspect.getfile(type(self))).resolve())
        self.id = self.process_path + ":" + self.short_id

        # ID of the step and process from which this process is called.
        # Both are None if this process is the root process.
        if main:
            step_id = None
            parent_process_id = None
        self.parent_process_id: str | None = parent_process_id
        self.step_id: str | None = step_id

        # Assign metadata attributes. Override in self.set_metadata().
        self.metadata: dict[str, str] = {}
        # self.label: str = "" # Human readable process name.
        # self.doc:   str = "" # Human readable process explaination.
        
        # Assign input/output dictionary attributes.
        # FIXME: dicts could use custom types like CWLTool does, instead of dicts.
        self.inputs:  dict = {} # Override in set_inputs()
        self.outputs: dict = {} # Override in set_outputs()

        # Maps input_id to its global source id, which is used as key in runtime_context
        self.input_to_source: dict[str, str] =  {}  # {input_id, global_source_id}
        
        # Assign requirements and hints.
        # Override in set_requirements().
        self.requirements:  dict = {}

        # NOTE: Not sure if I want to support hints, force requirements only?
        self.hints: dict = {}   
        
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
        self.runtime_context: dict[str, Value | Absent] = {}
        # self.runtime_context: dict[str, Any] = {}
        if main:
            self.loading_context = {}
            self.loading_context["graph"] = Graph() # Used in create_dependency_graph()
            self.loading_context["processes"] = {}  # {proc_id, process}
            self.process_cli_args(self.loading_context)

            # The YAML file uri comes from the first command-line argument.
            # self.runtime_context = self._load_input_object(sys.argv[1])
            self.runtime_context = self._load_input_object(self.loading_context["input_object"])
            # Copy system PATH environment variable
            self.loading_context["PATH"] = os.environ.get("PATH")
        else:
            if runtime_context is None:
                raise Exception(f"Subprocess {type(self)}({self.id}) is not initialized as root process, but lacks runtime context")
            if loading_context is None:
                raise Exception(f"Subprocess {type(self)}({self.id}) is not initialized as root process, but lacks loading context")
            self.runtime_context = runtime_context
            self.loading_context = loading_context

        # Register this process in the global cache
        self.loading_context["processes"][self.id] = self

        # Set runtime environment variables from main process
        # if main:


            # TODO is this used anywhere?
            # self.main_path = os.getcwd()


    def create_parser(self) -> argparse.ArgumentParser:
        """
        Create and return an argument parser for command-line arguments.

        TODO Description (schema) of arguments
        python process.py [--outdir OUTDIR] [--tmpdir TMPDIR] [--use_dask] input_object.yaml
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
            "--use_dask",
            action="store_true",
            default=False,
            help="Execute process tasks with Dask instead of with standard system call."
        )
        return parser
    
    
    def process_cli_args(self, loading_context: dict[str, Any]) -> None:
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
        print(f"[PROCESS]: Input object file:\n\t{input_object_path}")

        # Configure designated output directory
        out_dir_path = Path(args.outdir).absolute()
        if out_dir_path.exists() and not out_dir_path.is_dir():
            raise Exception(f"Output directory {out_dir_path} is not a directory")
        out_dir_path.mkdir(parents=True, exist_ok=True)            # Create tmp dir
        is_empty = not any(out_dir_path.iterdir())  # Check if out dir is empty
        loading_context["init_out_dir_empty"] = is_empty
        loading_context["designated_out_dir"] = out_dir_path
        print(f"[PROCESS]: Designated output directory:\n\t{loading_context['designated_out_dir']}")

        # Configure designated temporary directory
        tmp_dir_path = Path(args.tmpdir).absolute()
        if tmp_dir_path.exists() and not tmp_dir_path.is_dir():
            raise Exception(f"Temporary directory {tmp_dir_path} is not a directory")
        tmp_dir_path.mkdir(parents=True, exist_ok=True)            # Create tmp dir
        is_empty = not any(tmp_dir_path.iterdir())  # Check if tmp dir is empty
        loading_context["init_tmp_dir_empty"] = is_empty
        loading_context["designated_tmp_dir"] = tmp_dir_path
        print(f"[Process]: Designated temporary directory:\n\t{loading_context['designated_tmp_dir']}")

        # Configure whether Dask is used for execution
        loading_context["use_dask"] = args.use_dask
        print(f"[PROCESS]: Execute with Dask: {args.use_dask}")


    def global_id(self, s: str) -> str:
        """
        Concatenate the process ID and another string, split by a colon.
        """
        return self.id  + ":" + s


    def _load_yaml(self, yaml_uri: str) -> dict:
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
            if not isinstance(y, dict):
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
            class: file
            path: path/to/file

        input_files: 
            - {class: file, path: path/to/file1} 
            - {class: file, path: path/to/file2}

        Arguments:
            input_value: An object value from the primary key-value YAML layer.

        Returns:
            The extracted value.
        """
        expected_cwl_types: Sequence[str]

        if not isinstance(self.inputs[input_id]["type"], Sequence):
            expected_cwl_types = [self.inputs[input_id]["type"]]
        else:
            expected_cwl_types = self.inputs[input_id]["type"]


        if isinstance(input_value, Sequence):
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

            # Match input to schema
            value_types = PYTHON_CWL_T_MAPPING[type(head)]
            matched_types = [t for t in value_types if t in expected_cwl_types]
            if len(matched_types) == 0:
                raise Exception(f"Input '{input_id}' did not match the input schema")

            if isinstance(head, str):
                # Files and directories can come in the form of a string path,
                # which we need to check for.
                if "file" in matched_types:
                    return Value([FileObject(p) for p in input_value], FileObject, "file")
                if "directory" in matched_types:
                    return Value([DirectoryObject(p) for p in input_value], DirectoryObject, "directory")
            
            if isinstance(head, Mapping):
            # A mapping either means a potential file/directory, or an
            # unsupported custom data type. Unsupported types result in error.
                if "class" in head:
                    if "file" in head["class"]:
                        return Value(
                            [FileObject(p["path"]) for p in input_value], 
                            FileObject, 
                            "file"
                        )
                    elif "directory" in head["class"]:
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
                PYTHON_CWL_T_MAPPING[type(head)][0] # Mapping isnt 1-to-1, so take first item
            )
        elif isinstance(input_value, Mapping):
            # A mapping either means a potential file/directory, or an
            # unsupported custom data type. Unsupported types result in error.
            if "class" in input_value:
                if "file" in input_value["class"]:
                    return Value(
                        FileObject(input_value["path"]), 
                        FileObject, 
                        "file"
                    )
                elif "directory" in input_value["class"]:
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
            if type(input_value) not in PYTHON_CWL_T_MAPPING:
                raise Exception(f"Found unsupported Python value type {type(input_value)} for input {input_id}")

            # Match input to schema
            value_types = PYTHON_CWL_T_MAPPING[type(input_value)]
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
                PYTHON_CWL_T_MAPPING[type(input_value)][0]
            )


        # if isinstance(input_value, str):
        #     if 'file' in self.inputs[input_id]["type"]:
        #         input_value = FileObject(input_value)
        #     return input_value
        # elif isinstance(input_value, list):
        #     # Because file lists are represented as lists of dicts, we need to
        #     # check if a list contains files and extract their paths.
        #     # If the list is a regular list, we just return it.
        #     if len(input_value) == 0:
        #         return []

        #     _t = type(input_value[0])

        #     # Rudimentary homogeneity check on list
        #     valid = all([isinstance(i, _t) for i in input_value])
        #     if not valid:
        #         raise Exception(f"Array for input '{input_id} not homogeneous")
            
        #     # If input value is a list of files, transform it into list of
        #     # FileObjects
        #     if (isinstance(input_value[0], dict) and
        #         "class" in input_value[0] and "file" in input_value[0]):
        #         return [FileObject(v["path"]) for v in input_value]
        #     if "file[]" in self.inputs[input_id]["type"]:
        #         # FIXME Collision between 'string' and 'file' type when value is a path string
        #         return [FileObject(string_path) for string_path in input_value]

        #     return input_value
        # elif isinstance(input_value, dict):
        #     if "file" in input_value["class"]:
        #         return FileObject(input_value["path"])
        # raise Exception(f"Unexpected value type {type(input_value)}")

    
    def _load_input_object(self, yaml_uri: str) -> dict:
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
            print("\t-", input_id, ":", input_value)
        print()

        return runtime_context


    @abstractmethod
    def set_metadata(self) -> None:
        """
        TODO Better description
        Must be overridden to assign process metadata attributes.
        """
        pass


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
        # Example:
        # 
        # 
        # 
        # pass


    @abstractmethod
    def register_input_sources(self) -> None:
        """
        TODO
        Cache the source of each input with an ID unique to each Process 
        instance. The mapping is saved under self.input_to_sources. The key of
        each input can be found by using self.global_id().
        """
        pass


    def build_namespace(self) -> dict[str, Any]:
        """
        Build a local namespace that can be used in eval() calls to evaluate
        expressions that access CWL namespaces, like 'inputs'.
        FIXME NOTE: Doesnt handle multityped input parameters correctly.
        """
        namespace: dict[str, Any] = {}

        # Create an object that holds the CWL 'inputs' namespace. This object
        # is used in the eval() call.
        # Example: If the process has an input 'input_fits', it can be
        #          accessed in the expression as 'inputs.input_fits'.
        # inputs = lambda: None       # Create empty object

        # TODO Other CWL namespaces, like 'self', 'runtime'?
        namespace["inputs"] = {}

        for input_id, _ in self.inputs.items():
        # for input_id, input_dict in self.inputs.items():
            # If runtime value is Absent, value is assigned None
            source = self.input_to_source[input_id]
            value = self.runtime_context[source].value
            namespace["inputs"][input_id] = value

            # if "file" in input_dict["type"]:
            #     # Create built-in file properties used in CWL expressions
            #     if "[]" in input_dict["type"]:
            #         # Array of files
            #         file_objects = [FileObject(p) for p in value]
            #         # setattr(inputs, input_id, file_objects)
            #         namespace["inputs"][input_id] = file_objects
            #     else:
            #         # Single file
            #         # setattr(inputs, input_id, FileObject(value[0]))
            #         # NOTE: why is value[0] used here, instead of value?
            #         namespace["inputs"][input_id] = FileObject(value[0])
            # elif "string" in input_dict["type"]:
            #     # setattr(inputs, input_id, value)
            #     namespace["inputs"][input_id] = value
            # else:
            #     raise NotImplementedError(f"Input type {input_dict['type']} not supported")
            
        # namespace["inputs"] = inputs

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
            # NOTE Should None be 'null'?
            context_vars.update({"self": None})

        if type(expression) is not str:
            raise Exception(f"Expected expression to be a string, but found {type(expression)}")
        
        # Evaluate expression. Evaluating can return any type     
        if expression.startswith("$(") and expression.endswith(")"):
            # Expression is a Javascript expression
            # Build JS context with CWL namespaces
            js_context = js2py.EvalJs(context_vars)
            # context["self"] = ... # TODO Other CWL namespaces

            # Evaluate expression with JS engine
            ret = js_context.eval(expression[2:-1])
            if verbose: 
                print(f"[EVAL]: '{expression}' ({type(expression)}) -> '{ret}' ({type(ret)})")
        elif expression.startswith("$") and expression.endswith("$"):
            # Expression is a Python expression
            # Build context dictionary with CWL namespaces
            py_context = context_vars.copy()
            py_context["inputs"] = dict_to_obj(context_vars["inputs"])

            # Evaluate expression with Python eval()
            ret = eval(expression[1:-1], py_context)
            if verbose: 
                print(f"[EVAL]: '{expression}' ({type(expression)}) -> '{ret}' ({type(ret)})")
        else:
            # Expression is a plain string and doesn't need evaluating
            ret = expression
            if verbose: 
                print(f"[EVAL]: '{expression}' ({type(expression)}) -> '{ret}' ({type(ret)})")

        return ret



#########################################
#                 Node                  #
#########################################
class Node:
    def __init__(
            self,
            id: str,
            processes: list[BaseProcess],
            parents: Optional[list[str]] = None,    #list[parent_ids]
            children: Optional[list[str]] = None,   #list[child_ids]
            # internal_dependencies and graph are used in graph optimization 
            is_tool_node: bool = False,
            internal_dependencies: Optional[dict[str, str | list[str]]] = None,  #{node_id: node_id}
            graph: Optional['Graph'] = None
        ) -> None:
        """
        Node containing one or more processes. Stores dependencies between a
        node and its parents/children and between the processes in 
        self.processes.
        """
        self.id = id
        self.is_tool_node: bool = is_tool_node
        self.processes: list[BaseProcess] = processes

        if parents is None:
            parents = []
        self.parents: list[str] = parents

        if children is None:
            children = []
        self.children: list[str] = children

        if not is_tool_node:
            if graph is None:
                if internal_dependencies is None:
                    internal_dependencies = {}

                graph = self.create_task_graph(
                    internal_dependencies,
                    processes
                )
        self.graph: Graph | None = graph


    def __deepcopy__(self, memo) -> 'Node':
        """
        Make a deep copy of the node and return it.
        NOTE: Creates copies are reference to the underlying processes,
        instead of creating new processes. This ommits initializing
        processes again, which is pointless and takes time.
        """
        # NOTE Untested
        processes = self.processes.copy() # << Shallow copy!
        # processes = [p for p in self.processes] # << Shallow copy!
        parents = deepcopy(self.parents)
        children = deepcopy(self.children)
        graph = deepcopy(self.graph)
        node = Node(
            self.id, 
            processes = processes, 
            parents = parents, 
            children = children,
            is_tool_node = self.is_tool_node,
            graph = graph
        )
        return node
    

    def create_task_graph(
            self,
            dependencies: dict[str, Union[str, list[str]]],
            processes: list[BaseProcess]
        ) -> 'Graph':
        """
        Arguments:
            dependencies: Maps parent IDs to child IDs.
            processes: Processes to be contained by nodes. 

        Returns:
            Task graph with nodes that hold a single CommandLineTool.
        """
        graph = Graph()

        # Add nodes, but don't connect them yet
        for process in processes:
            graph.add_node(
                Node(
                    process.id, 
                    processes = [process], 
                    is_tool_node = True
                )
            )

        # Connect nodes
        for node_id, child_ids in dependencies.items():
            if isinstance(child_ids, str):
                child_ids = [child_ids]
            elif not isinstance(child_ids, list):
                raise Exception(f"Expected str or list, but found {type(child_ids)}")
            graph.connect_node(node_id, children=child_ids)
        return graph
    

    def merge(
            self,
            nodes: Union['Node', list['Node']]
        ) -> 'Node':
        # self.merged = True
        # 
        raise NotImplementedError()

    
    def is_leaf(self) -> bool:
        return len(self.children) == 0
    
    def is_root(self) -> bool:
        return len(self.parents) == 0



#########################################
#                 Graph                 #
#########################################
class Graph:
    def __init__(
            self, 
            # grouping: bool = False
        ) -> None:
        """
        BUG where a node is added twice to parents/children
        Directed Acyclic Graph (DAG) implementation to represent a workflow
        task graph. Can be used to optimize task graph execution.
        """
        self.roots: list[str] = []  # [node_ids]
        self.leaves: list[str] = [] # [node_ids]
        self.nodes: dict[str, Node] = {}    # {node_id, Node}
        self.in_deps: dict[str, list[str]] = {}  # {node_id: [parent_ids]}
        self.out_deps: dict[str, list[str]] = {}  # {node_id: [child_ids]}
        self.size: int = 0
        # self.grouping: bool = grouping
        
        # Create placeholder IDs for nodes to improve readability
        self._next_id: int = 0
        self.short_id: dict[str, int] = {}

    
    def __deepcopy__(self, memo) -> 'Graph':
        """
        Make a deep copy of the graph and return it.
        NOTE: Creates shallow copies of nodes instead. This ommits initializing
        processes again, which is pointless and takes time.
        """
        graph = Graph()
        graph.roots = self.roots.copy()
        graph.leaves = self.leaves.copy()
        graph.nodes = self.nodes.copy() # << processes in nodes are references to originals!
        graph.in_deps = self.in_deps.copy()
        graph.out_deps = self.out_deps.copy()
        graph._next_id = self._next_id
        graph.short_id = self.short_id.copy()    # {node_id: simple_id}
        # graph.roots = deepcopy(self.roots)
        # graph.leaves = deepcopy(self.leaves)
        # graph.nodes = deepcopy(self.nodes) # << processes in nodes are references to originals!
        # graph.in_deps = deepcopy(self.in_deps)
        # graph.out_deps = deepcopy(self.out_deps)
        # graph._next_id = self._next_id
        # graph.short_id = deepcopy(self.short_id)    # {node_id: simple_id}
        return graph

    
    def __str__(self) -> str:
        """
        Construct a string containing graph info and return it. Simple node
        IDs are used to improve clarity. Node IDs are mapped to simple node
        IDs in self.short_id.

        Returns a string containing:
            - Root nodes
            - Edges
            - Leaf nodes
        """
        s = "nodes[parents/children]: "
        for node_id in self.nodes:
            s += f"{self.short_id[node_id]}["
            if node_id in self.in_deps:
                s += f"{','.join([str(self.short_id[p_id]) for p_id in self.in_deps[node_id]])}"
            else:
                s += "."
            s += "/"
            if node_id in self.out_deps:
                s += f"{','.join([str(self.short_id[c_id]) for c_id in self.out_deps[node_id]])}"
            else:
                s += "."
            s += "] "
        s += "\nroots: " 
        for root_id in self.roots:
            s += f"{self.short_id[root_id]} "
        s += "\nedges: \n"
        for node_id, child_id in self.out_deps.items():
            for child in child_id:
                s += f"\t{self.short_id[node_id]} -> {self.short_id[child]}\n"
        s += "leaves: " 
        for leaf_id in self.leaves:
            s += f"{self.short_id[leaf_id]} "
        return s

    
    def print(self) -> None:
        """ Print graph information. """
        print()
        print(self)
        print()

    
    def next_id(self) -> int:
        """ Generate and update the next available simple node ID. """
        self._next_id += 1
        return self._next_id - 1
    

    def add_node(
            self,
            node: Node
        ) -> None:
        """
        Add a node to the graph, without adding edges to other nodes.
        NOTE: For connecting nodes with edges, see connect_node().
        """
        if node.id in self.nodes:
            raise Exception(f"Node ID already exists in graph. Invalid ID: {node.id}")
        
        self.nodes[node.id] = node
        self.roots.append(node.id)
        self.leaves.append(node.id)
        self.short_id[node.id] = self.next_id()
        self.size += 1


    def connect_node(
            self,
            node_id: str,
            parents: Optional[list[str]] = None,
            children: Optional[list[str]] = None
        ) -> None:
        """
        Add edges between a node and its parents/children.

        Arguments:
            node_id: ID of the node.
            parents: List containing node IDs of parent nodes to connect to.
            children: List containing node IDs of child nodes to connect to.
        """
        if node_id not in self.nodes:
            raise Exception(f"Graph does not contain node with ID {node_id}")

        node = self.nodes[node_id]

        if parents is None:
            parents = []
        node.parents.extend(parents)
        node.parents = list(set(node.parents))   # Removes duplicates

        if children is None:
            children = []
        node.children.extend(children)
        node.children = list(set(node.children)) # Removes duplicates

        for parent_id in node.parents:
            # Add node as child to parent
            self.nodes[parent_id].children.append(node.id)
            
            # Register in-dependencies
            if node.id in self.in_deps:
                self.in_deps[node.id].append(parent_id)
            else:
                self.in_deps[node.id] = [parent_id]
            
            # Register out-dependencies for parent
            if parent_id in self.out_deps:
                self.out_deps[parent_id].append(node.id)
            else:
                self.out_deps[parent_id] = [node.id]

            # Parent cannot be a leaf
            if parent_id in self.leaves:
                self.leaves.remove(parent_id)

        for child_id in node.children:
            # Add node as parent to child
            self.nodes[child_id].parents.append(node.id)
            
            # Register in-dependencies for child
            if child_id in self.in_deps:
                if not node.id in self.in_deps[child_id]:
                    self.in_deps[child_id].append(node.id)
            else:
                self.in_deps[child_id] = [node.id]
            
            # Register out-dependencies
            if node.id in self.out_deps:
                if not child_id in self.out_deps[node.id]:
                    self.out_deps[node.id].append(child_id)
            else:
                self.out_deps[node.id] = [child_id]

            # child cannot be a root
            if child_id in self.roots:
                self.roots.remove(child_id)

        # Update graph root dict
        if node.is_root():
            if node.id not in self.roots:
                self.roots.append(node.id)
        elif node.id in self.roots:
                self.roots.remove(node.id)

        # Update graph leaf dict
        if node.is_leaf():
            if node.id not in self.leaves:
                self.leaves.append(node.id)
        elif node.id in self.leaves:
            self.leaves.remove(node.id)

    
    def remove_node(
            self,
            node_id: str
        ) -> None:
        """
        TODO Description
        Remove a node
        """
        self.roots.remove(node_id)
        self.leaves.remove(node_id)
        self.nodes.pop(node_id)
        self.size -= 1


    def merge(
            self,
            node_ids: str | list[str]
        ):
        """
        TODO Description
        Merge nodes 
        """
        if isinstance(node_ids, str):
            node_ids = [node_ids]
            
        # Create new_node with id = {short_id0}:{short_id1}:...
        new_id: str = ":".join([str(self.short_id[node_id]) for node_id in node_ids])
        new_node = Node(new_id, [], is_tool_node = False)
        self.add_node(new_node)

        # Merge nodes into new node
        new_node.merge([self.nodes[node_id] for node_id in node_ids])

        # Remove old nodes
        for node_id in node_ids:
            self.remove_node(node_id)
        return new_node
    

    def get_nodes(
            self,
            node_ids: str | list[str]
        ) -> list[Node]:
        """
        TODO
        """
        if isinstance(node_ids, str):
            node_ids = [node_ids]
        elif not isinstance(node_ids, list):
            raise Exception(f"Expected str or list type, but got {type(node_ids)}")

        return [self.nodes[node_id] for node_id in node_ids]