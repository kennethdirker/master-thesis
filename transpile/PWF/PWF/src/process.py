import copy
# import dask.delayed
import inspect
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
from typing import Any, Optional, Union

from .utils import Absent, FileObject


class BaseProcess(ABC):
    def __init__(
            self,
            main: bool = False,
            client: Optional[Client] = None,
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
        self.process_path: str = inspect.getfile(type(self))
        self.id = self.process_path + ":" + self.short_id

        # ID of the step and process from which this process is called.
        # Both are None if this process is the root process.
        if main:
            step_id = None
            parent_process_id = None
        self.parent_process_id: str = parent_process_id
        self.step_id: str = step_id

        # Assign metadata attributes. Override in self.set_metadata().
        self.label: str = "" # Human readable process name.
        self.doc:   str = "" # Human readable process explaination.
        
        # Assign input/output dictionary attributes.
        # FIXME: dicts could use classes like CWLTool does, instead of dicts.
        self.inputs:  dict = {} # Override in set_inputs()
        self.outputs: dict = {} # Override in set_outputs()

        # Maps input_id to its global source id, which is used as key in runtime_context
        self.input_to_source: dict[str, str] =  {}  # {input_id, global_source_id}
        
        # Assign requirements and hints.
        # Override in set_requirements().
        # NOTE: Probably not needed for Minimal Viable Product.
        self.requirements:  list = []
        self.hints: list = []
        
        # TODO Update description
        # Assign a dictionary with runtime input variables and a dictionary to
        # map processes and step IDs. Only the root process must load the input
        # YAML from a file. Non-root processes get a reference to the
        # dictionary loaded in the main process.
        self.runtime_context: dict[str, Any] = {}
        if main:
            # The YAML file uri comes from the first command-line argument.
            self.runtime_context = self._load_input_object(sys.argv[1])

            self.loading_context = {}
            self.loading_context["graph"] = Graph() # Used in create_dependency_graph()
            self.loading_context["processes"] = {}  # {proc_id, process}
        else:
            if runtime_context is None:
                raise Exception(f"Subprocess {type(self)}({self.id}) is not initialized as root process, but lacks runtime context")
            if loading_context is None:
                raise Exception(f"Subprocess {type(self)}({self.id}) is not initialized as root process, but lacks loading context")
            self.runtime_context = runtime_context
            self.loading_context = loading_context

        # Register this process in the global cache
        self.loading_context["processes"][self.id] = self

        # Register path of main
        if main:
            self.main_path = os.getcwd()

        # Prepare Dask client
        # NOTE Can easilty be replaced by other Dask clients
        if client is None:
            client = Client()
        self.client: Client = client

        # Used in create_task_graph()
        # self.task_graph_ref: Union[Delayed, None] = None
    

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
        
    
    def resolve_input_value(self, input_value: dict) -> Union[str, list[str]]:
        """
        Extract a value from a key-value entry. This is needed because CWL
        input objects may contain key-value pairs that are more complicated
        than needed... Below is an example, where we need to extract the path.

        # Example 
        # id:
        #     class: file
        #     path: path/to/some/file

        Arguments:
            input_value: A YAML entry.

        Returns
        """
        if isinstance(input_value, str):
            return input_value
        elif isinstance(input_value, list):
            return input_value
        elif isinstance(input_value, dict):
            arg_type = input_value["class"]
            if "file" in arg_type:
                return input_value["path"]
        raise Exception(f"Unexpected value type {type(input_value)}")

    
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

        print("Inputs loaded into runtime context:")
        for input_id, input_value in input_obj.items():
            # Input from object is indexed by {Process.id}:{input_id}
            input_value = self.resolve_input_value(input_value)
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
    
    
    # def _process_inputs(self) -> None:
    #     """
    #     TODO Better description
    #     """
    #     # NOTE: This is needed for dynamic I/O
    #     # FIXME: Find a better way to support dynamic I/O?
    #     # Create an entry in the runtime_context dict for each input argument.
    #     # The process ID is prepended to the input ID to ensure global
    #     # uniqueness of input IDs.
    #     for input_id, input_dict in self.inputs.items():
    #         if self.global_id(input_id) not in self.runtime_context:
    #             value = Absent()
    #             if "default" in input_dict:
    #                 value = input_dict["default"]
    #             self.runtime_context[self.global_id(input_id)] = value
    

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
        This function can be overridden to indicate execution requirements.
        """
        # Example:
        # 
        # 
        # 
        # pass
        return {}


    # @abstractmethod
    # def create_dependency_graph(self) -> None:
    #     """ 
    #     TODO Better description
    #     FIXME This is not accurate anymore, rewrite!
    #     This function must be overridden to implement building the Dask Task
    #     Graph. The function must assign the final graph node to 
    #     'self.task_graph_ref', which is executed by 'self.execute()'.
    #     """
    #     # Example:
    #     # 
    #     # 
    #     # 
    #     pass


    # @abstractmethod
    # def create_task_graph(self) -> None:
    #     """
    #     TODO Desc
    #     """
    #     pass


    @abstractmethod
    def register_input_sources(self) -> None:
        """
        TODO
        """
        pass


    @abstractmethod
    def execute(self):
        """
        TODO Better description
        """
        # self.task_graph_ref.compute()

    def __call__(self):
        """
        TODO
        """
        self.execute()


    # def runnable(self) -> bool:
    #     """
    #     TODO Better description
    #     Check whether a process is ready to be executed.

    #     Returns:
    #         True if the process can be run, False otherwise.
    #         Additionally, a list of missing inputs is returned.
    #     """
    #     # TODO: Currently only checks that all keys are matched. Additions: 
    #     # - Validate type of inputs.
    #     # - 
    #     # # NOTE: Extra checks probably not needed for Minimal Viable Product.

    #     green_light = True
    #     missing_inputs: list[str] = []
    #     for input_id, input_dict in self.inputs.items():
    #         if input_id not in self.runtime_context or \
    #            isinstance(input_dict, Absent):
    #             green_light = False
    #             missing_inputs.append(input_id)
    #     return green_light, missing_inputs


    def eval(
            self, 
            expression: str
        ) -> str:
        """
        Evaluate an expression. The expression may access CWL namespace
        variables, like 'inputs'.
        TODO Extend supported CWL namespace variables
        """

        if not expression.startswith("$") and not expression.endswith("$"):
            # Expression is a plain string and doesn't need evaluating.
            return expression


        local_vars: dict[str, Any] = {}

        # Build inputs object with built-in file properties
        inputs = lambda: None
        for input_id, input_dict in self.inputs.items():
            if "file" in input_dict["type"]:
                if "[]" in input_dict["type"]:
                    raise NotImplementedError("Arrays not supported")
                
                source = self.input_to_source[input_id]
                path = self.runtime_context[source]
                setattr(inputs, input_id, FileObject(path))
        local_vars["inputs"] = inputs

        return eval(expression, local_vars)
    

    # def eval(self, s: str):
    #     """
    #     TODO Desc
    #     Evaluate
    #     """
    #     if s.startswith("$") and s.endswith("$"):
    #         source = s[1:-1]
    #         global_input_id = self.input_to_source[source]
    #         value = self.runtime_context[global_input_id]
            
    #         if isinstance(value, Absent):
    #             raise Exception("Missing paramter ", global_input_id)
    #         return value
    #     return s

        #     if "/" in source:
        #         # From local step 
        #         raise NotImplementedError()
        #     else:
        #         # From local input
        #         global_input_id = self.input_to_source[self.global_id(source)]
        #         value = self.runtime_context[global_input_id]
        #         if isinstance(value, Absent):
        #             raise Exception("Missing paramter ", global_input_id)
        #         else:
        #             return value
        # else:
        #     return s


# class NodeStatus(Enum):
#     WAITING = 0
#     RUNNABLE = 1
#     RUNNING = 2
#     COMPLETED = 3


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
            internal_dependencies: Optional[dict[str, str]] = None,  #{node_id: node_id}
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
        self.graph: Graph = graph


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
        NotImplementedError()

    
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
        IDs in self.id_mapping.

        Returns a string containing:
            - Root nodes
            - Edges
            - Leaf nodes
        """
        s = "nodes: "
        for node_id in self.nodes:
            s+= f"{self.short_id[node_id]} "
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
        new_id: str = ":".join(
            [self.short_id[node_id] for node_id in node_ids]
        )
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