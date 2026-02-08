
from copy import deepcopy
from typing import Dict, List, Optional, Union

from .process import BaseProcess


#########################################
#                 Node                  #
#########################################
class Node:
    def __init__(
            self,
            id: str,
            processes: List[BaseProcess],
            parents: Optional[List[str]] = None,    #List[parent_ids]
            children: Optional[List[str]] = None,   #List[child_ids]
            # internal_dependencies and graph are used in graph optimization 
            is_tool_node: bool = False,
            internal_dependencies: Optional[Dict[str, str | List[str]]] = None,  #{node_id: node_id}
            graph: Optional['Graph'] = None
        ) -> None:
        """
        Node containing one or more processes. Stores dependencies between a
        node and its parents/children and between the processes in 
        self.processes.
        """
        self.id = id
        self.is_tool_node: bool = is_tool_node
        self.processes: List[BaseProcess] = processes

        if parents is None:
            parents = []
        self.parents: List[str] = parents

        if children is None:
            children = []
        self.children: List[str] = children

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
            dependencies: Dict[str, Union[str, List[str]]],
            processes: List[BaseProcess]
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
            nodes: Union['Node', List['Node']]
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
        self.roots: List[str] = []  # [node_ids]
        self.leaves: List[str] = [] # [node_ids]
        self.nodes: Dict[str, Node] = {}    # {node_id, Node}
        self.in_deps: Dict[str, List[str]] = {}  # {node_id: [parent_ids]}
        self.out_deps: Dict[str, List[str]] = {}  # {node_id: [child_ids]}
        self.size: int = 0
        # self.grouping: bool = grouping
        
        # Create placeholder IDs for nodes to improve readability
        self._next_id: int = 0
        self.short_id: Dict[str, int] = {}

    
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
            parents: Optional[List[str]] = None,
            children: Optional[List[str]] = None
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
            node_ids: str | List[str]
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
            node_ids: str | List[str]
        ) -> List[Node]:
        """
        TODO
        """
        if isinstance(node_ids, str):
            node_ids = [node_ids]
        elif not isinstance(node_ids, list):
            raise Exception(f"Expected str or list type, but got {type(node_ids)}")

        return [self.nodes[node_id] for node_id in node_ids]