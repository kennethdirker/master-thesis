import copy
import dask.delayed
import inspect
import sys
import uuid
import yaml

from abc import ABC, abstractmethod
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional, Union

from .utils import Absent
# from .Workflow import BaseWorkflow

class BaseProcess(ABC):
    def __init__(
            self,
            main: bool = False,
            runtime_context: Optional[dict[str, Any]] = None,
            loading_context: Optional[dict[str, Any]] = None,
            parent_id: Optional[str] = None,
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
        # A process that is called from the command line is root. Processes
        # that are instantiated from other another process (sub-processes)
        # are not root.
        self.is_root: bool = main

        # Unique identity of a process instance. The id looks as follows:
        #   "{path/to/process/script/file}:{uuid}"  
        self._id = inspect.getfile(type(self)) + ":" + str(uuid.uuid4())

        # ID of the step and process from which this process is called.
        # Both are None if this process is the root process.
        if main:
            step_id = None
            parent_id = None
        self.parent_id = parent_id
        self.step_id = step_id

        # print(f"Created process with id '{self._id}'")
        
        # Assign metadata attributes. Override in self.metadata().
        self.label: Union[str, None] = None # Human readable process name.
        self.doc:   Union[str, None] = None # Human readable process explaination.
        
        # Assign input/output dictionary attributes.
        # FIXME: dicts could use classes like CWLTool does, instead of dicts.
        self.inputs_dict:  Union[dict] = {} # Override in self.inputs()
        self.outputs_dict: Union[dict] = {} # Override in self.outputs()
        
        # TODO What to do with self.parents / self.children???
        self.parents: list[Any] = []
        self.children: list[Any] = []

        # Assign requirements and hints.
        # Override in self.requirements() and self.hints().
        # NOTE: Probably not needed for Minimal Viable Product.
        self.reqs:  Union[dict, None] = {}
        self.hints: Union[dict, None] = {}
        
        # Assign a dictionary with runtime input variables and a dictionary to
        # map process and step IDs. Only the root process must load the input
        # YAML from a file. Non-root processes get a reference to the
        # dictionary loaded in the main process.
        if main:
            # The YAML file uri comes from the first command line argument
            self.runtime_context = self._load_yaml(sys.argv[1])
            print("Inputs loaded into self.runtime_context:")
            for k, v in self.runtime_context.items():
                print("\t- ", k, ":", v)
            print()

            self.loading_context = {}
            self.loading_context["graph"] = Graph() # Used in self.create_dependency_graph()
            self.loading_context["processes"] = {}  # {proc_id, process}
            self.loading_context["inputs"] = {}     # {, }
        else:
            if runtime_context is None:
                raise Exception(f"Subprocess {type(self)}({self._id}) is not initialized as root process, but lacks runtime context")
            if loading_context is None:
                raise Exception(f"Subprocess {type(self)}({self._id}) is not initialized as root process, but lacks loading context")
            self.runtime_context = runtime_context
            self.loading_context = loading_context

        # Register this process in the global cache
        self.loading_context["processes"][self._id] = self


        # TODO: Prepare Dask?
        # dask_client = ...
        # Used in self.create_task_graph()
        self.task_graph_ref: Union[dask.Delayed, None] = None



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


    @abstractmethod
    def metadata(self) -> None:
        """
        TODO Better description
        Must be overridden to assign process metadata attributes.
        """
        pass


    # @abstractmethod
    def inputs(self) -> None:
        """ 
        TODO Better description
        This function must be overridden to define input job order field 
        requirements. These job order requirements are used to test whether a 
        tool is ready to be executed.
        """
        # Example:
        # self.inputs_dict = {
        #     "url_list": {
        #         "type": "file"
        #     }
        # }
        pass
    
    
    def _process_inputs(self) -> None:
        """
        TODO Better description
        """
        # NOTE: This is needed for dynamic I/O
        # FIXME: Find a better way to support dynamic I/O?
        # Create an entry in the runtime_context dict for each input argument.
        # The process ID is prepended to the input ID to ensure global
        # uniqueness of input IDs.
        for input_id, input_dict in self.inputs_dict.items():
            if input_id not in self.runtime_context:
                self.runtime_context[self.global_id(input_id)] = Absent()
            # global_inputs = self.loading_context["inputs"]
            # global_inputs[self.global_id(input_id)]
    

    @abstractmethod
    def outputs(self) -> None:
        """
        TODO Better description
        This function must be overridden to define Process outputs.
        """
        # Example:
        # self.outputs_dict = {
        #     "before_noise_remover": {
        #         "type": "file",
        #         # "outputSource": inputs/{input_arg_id}
        #         # "outputSource": {step_id}/{step_output_id}
        #         "outputSource": "imageplotter/output"
        #     }
        # }
        pass


    # @abstractmethod
    def requirements(self) -> None:
        """
        TODO Better description
        This function can be overridden to indicate execution requirements.
        """
        # Example:
        # 
        # 
        # 
        # pass
        return {}


    @abstractmethod
    def create_dependency_graph(self) -> None:
        """ 
        TODO Better description
        FIXME This is not accurate anymore, rewrite!
        This function must be overridden to implement building the Dask Task
        Graph. The function must assign the final graph node to 
        'self.task_graph_ref', which is executed by 'self.execute()'.
        """
        # Example:
        # 
        # 
        # 
        pass


    @abstractmethod
    def create_task_graph(self) -> None:
        pass
    

    def execute(self):
        """
        TODO Better description
        """
        runnable, missing = self.runnable()
        if runnable:
            self.task_graph_ref.compute()
        else:
            raise RuntimeError(
                f"{self.id} is missing inputs {missing} and cannot run")


    def __call__(self, runtime_dict: dict):
        self.execute(runtime_dict)


    def runnable(self) -> bool:
        """
        TODO Better description
        Check whether a process is ready to be executed.

        Returns:
            True if the process can be run, False otherwise.
            Additionally, a list of missing inputs is returned.
        """
        # TODO: Currently only checks that all keys are matched. Additions: 
        # - Validate type of inputs.
        # - 
        # # NOTE: Extra checks probably not needed for Minimal Viable Product.
            
        green_light = True
        missing_inputs: list[str] = []
        for key, value in self.inputs_dict.items():
            if key not in self.runtime_context or \
               isinstance(value, Absent):
                green_light = False
                missing_inputs.append(key)
        return green_light, missing_inputs
    

    def global_id(
            self,
            s: str
        ) -> str:
        """
        Concatenate the process ID and another string, split by a colon.
        """
        return self._id  + ":" + s


    def get_tool_children(self) -> list[str]:
        # TODO Needed?
        pass



class Node:
    def __init__(
            self,
            id: str,
            parents: Optional[list[str]] = None,    #list[parent_ids]
            children: Optional[list[str]] = None,   #list[child_ids]
            processes: Optional[list[BaseProcess]] = None,
            internal_dependencies: Optional[dict[str, str]] = None  #{node_id: node_id}
        ) -> None:
        """
        TODO class description
        """
        
        self.id = id

        # self.merged = False

        if parents is None:
            parents = []
        self.parents = parents

        if children is None:
            children = []
        self.children = children

        if processes is None:
            processes = []
        self.processes = processes

        if internal_dependencies is None:
            internal_dependencies = {}
        self.internal_dependencies = internal_dependencies


    def __deepcopy__(self) -> 'Node':
        node = Node(self.id)
        node.parents = deepcopy(self.parents)
        node.children = deepcopy(self.children)
        node.processes = [p for p in self.processes] # << Not a deepcopy!
        node.internal_dependencies = deepcopy(self.internal_dependencies)


    def merge(
            self,
            nodes: Union['Node', list['Node']]
        ) -> 'Node':
        # self.merged = True
        NotImplementedError()

    
    def is_leaf(self):
        return len(self.children) == 0
    
    def is_root(self):
        return len(self.parents) == 0


class Graph:
    def __init__(
            self, 
            # grouping: bool = False
        ) -> None:
        """
        TODO class description 
        """
        self.roots: list[str] = []  # [node_ids]
        self.leaves: list[str] = [] # [node_ids]
        self.nodes: dict[str, Node] = {}    # {node_id, Node}
        self.in_deps: dict[str, list[str]] = {}  # {node_id: [parent_ids]}
        self.out_deps: dict[str, list[str]] = {}  # {node_id: [child_ids]}
        # self.grouping: bool = grouping

    
    def __deepcopy__(self) -> 'Graph':
        graph = Graph()
        graph.roots = deepcopy(self.roots)
        graph.leaves = deepcopy(self.leaves)
        graph.nodes = deepcopy(self.nodes) # << processes in nodes are refs to originals!
        graph.in_deps = deepcopy(self.in_deps)
        graph.out_deps = deepcopy(self.out_deps)


    def add_node(
            self,
            node: Node
        ):
        """
        TODO Description
        """
        if node.id in self.nodes:
            raise Exception(f"Node ID already exists in graph. Invalid ID: {node.id}")

        # Add node to graph
        self.nodes[node.id] = node

        # Add the new node to its parents and children
        # Update parent nodes
        if node.is_root():
            self.roots.append(node.id)
        else:
            for parent_id in node.parents:
                # Register in-dependencies
                self.nodes[parent_id].children.append(node.id)
                self.in_deps[node.id] = parent_id

                # Check if this node replaces its parent as leaf
                if self.nodes[parent_id].is_leaf():
                    self.leaves.remove(parent_id)

        # Update child nodes
        if node.is_leaf():
            self.leaves.append(node.id)
        else:
            for child_id in node.children:
                # Register out-dependencies
                self.nodes[child_id].parents.append(node.id)
                self.out_deps[node.id] = child_id

                # Check if this node replaces its child as root
                if self.nodes[child_id].is_root():
                    self.roots.remove(child_id)


    def tie_leaves(self) -> None:
        """
        Connect all leaf nodes to a final 'knot' node.
        """
        self.add_node(Node(id = "knot", parents=self.leaves))


    
    def remove_node(
            self,
            node_id: str
        ) -> None:
        """
        
        """
        raise NotImplementedError()


    def merge(
            self,
            node_ids: str | list[str]
        ):
        """
        TODO Description
        Merge nodes 
        """
        raise NotImplementedError()
            
