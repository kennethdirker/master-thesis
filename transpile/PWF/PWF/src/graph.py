from __future__ import annotations

from abc import ABC

from copy import deepcopy
from typing import cast, Dict, Iterator, List, Optional, Set, Tuple, Union
from uuid import uuid4

from .process import BaseProcess
from .commandlinetool import BaseCommandLineTool





class BaseNode(ABC):
    id: str
    short_id: Optional[int]
    parents: Dict[str, Node]
    children: Dict[str, Node]

    def __init__(
            self,
            id: Optional[str] = None,
            short_id: Optional[int] = None,
            parents: Optional[List[Node]] = None,
            children: Optional[List[Node]] = None,
        ):
        self.id = id if id else str(uuid4())
        self.short_id = short_id
        self.parents = {}
        self.add_parents(parents)
        self.children = {}
        self.add_children(children)


    def add_parents(
            self, 
            parents: Node | List[Node] | None
        ):
        if parents is None:
            return
        
        if isinstance(parents, Node):
            parents = [parents]
        
        self.parents.update({parent.id: parent for parent in parents})


    def add_children(
            self, 
            children: Node | List[Node] | None
        ):
        """
        
        """
        if children is None:
            return
        
        if isinstance(children, Node):
            children = [children]
        
        self.children.update({child.id: child for child in children})


    def is_root(self) -> bool:
        return len(self.parents) == 0

    def is_leaf(self) -> bool:
        return len(self.parents) == 0


class ToolNode(BaseNode):
    tool: BaseCommandLineTool
    
    def __init__(
            self,
            tool: BaseCommandLineTool,
            id: Optional[str] = None,
            short_id: Optional[int] = None,
            parents: Optional[List[ToolNode]] = None,
            children: Optional[List[ToolNode]] = None,
        ):
        id = id if id else tool.id
        super().__init__(id, short_id, parents, children) # type: ignore
        self.tool = tool


class OuterNode(BaseNode):
    graph: InnerGraph

    def __init__(
            self,
            id: Optional[str] = None,
            short_id: Optional[int] = None,
            parents: Optional[List[ToolNode]] = None,
            children: Optional[List[ToolNode]] = None,
            tools: Optional[BaseCommandLineTool | List[BaseCommandLineTool]] = None,
            di_edges: Optional[List[Tuple]] = None, # TODO specify type

        ):
        """
        
        """
        super().__init__(id, short_id, parents, children) # type: ignore
        self.graph = InnerGraph()

        if (tools or di_edges) and not (tools and di_edges):
            raise Exception(f"Either both ``tools`` and ``di_edges`` or neither must be provided")
        else:
            if isinstance(tools, BaseCommandLineTool):
                tools = [tools]
            assert tools is not None
            assert di_edges is not None
            tool_nodes = [ToolNode(t, t.id, ) for t in tools]
            self.graph.add_nodes(tool_nodes)    # type: ignore
            self.graph.add_edges(di_edges)  # TODO <- does this work?


    def merge_with(
            self,
            other: OuterNode
        ) -> OuterNode:
        raise NotImplementedError()


# Typedef
Node = ToolNode | OuterNode


class BaseGraph(ABC):
    size:      int
    nodes:     Dict[str, Node]
    roots:     List[str]
    leaves:    List[str]
    short_ids: Dict[str, int]
    in_edges:  Dict[str, List[str]]
    out_edges: Dict[str, List[str]]

    def __init__(self):
        self.size = 0
        self.nodes = {}
        self.roots = []
        self.leaves = []
        self.short_ids = {}
        self.in_edges = {}
        self.out_edges = {}


    def short_id(self) -> Iterator[int]:
        n: int = 0
        while True:
            yield n
            n += 1


    def add_nodes(self, nodes: Node | List[Node]) -> None:
        """
        Add one or more nodes to the graph.
        NOTE: This only adds a node. For edges, see ``add_edges()``.
        """
        if isinstance(nodes, Node):
            nodes = [nodes]
        for node in nodes:
            if node.id in self.nodes:
                raise Exception(f"Graph already contains a node with id {node.id}")
            self.nodes[node.id] = node
            self.roots.append(node.id)
            self.leaves.append(node.id)
            short_id = next(self.short_id())
            self.short_ids[node.id] = short_id
            node.short_id = short_id
            self.size += 1


    def add_edges(
            self, 
            di_edges: List[Tuple[Node, Node]]
        ):
        """
        
        """
        for node_a, node_b in di_edges:
            # Register edges in graph
            if node_a.id in self.out_edges:
                self.out_edges[node_a.id].append(node_b.id)
            else:
                self.out_edges[node_a.id] = [node_b.id]

            if node_b.id in self.out_edges:
                self.in_edges[node_b.id].append(node_a.id)
            else:
                self.in_edges[node_b.id] = [node_a.id]

            # Register edges in nodes
            node_a.add_children(node_b)
            node_b.add_parents(node_a)

            # Update roots and leaves
            if node_a.id in self.leaves:
                self.leaves.remove(node_a.id)
            if node_b.id in self.roots:
                self.roots.remove(node_b.id)
            if node_a.is_root() and node_a.id not in self.roots:
                self.roots.append(node_a.id)
            if node_b.is_leaf() and node_b.id not in self.roots:
                self.roots.append(node_b.id)


    def add_parents(
            self,
            node: Node,
            parents: List[Node | str]
        ) -> None:
        """
        
        """
        edges: List[Tuple[Node, Node]] = []
        for parent in parents:
            if isinstance(parent, Node):
                edges.append((parent, node))
            elif isinstance(parent, str):
                edges.append((self.nodes[parent], node))
            else:
                raise TypeError(f"Expected 'str' or 'Node', but found '{type(parent)}'")
        self.add_edges(edges)


    def add_children(
            self,
            node: Node,
            children: List[Node | str]
        ) -> None:
        """
        
        """
        edges: List[Tuple[Node, Node]] = []
        for child in children:
            if isinstance(child, Node):
                edges.append((node, child))
            elif isinstance(child, str):
                edges.append((node, self.nodes[child]))
            else:
                raise TypeError(f"Expected 'str' or 'Node', but found '{type(child)}'")
        self.add_edges(edges)

    
    def replace(
            self,
            targets: Node | List[Node],
            substitute: Node
        ):
        """
        
        """
        raise NotImplementedError()


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
            s += f"{self.short_ids[node_id]}["
            if node_id in self.in_edges:
                s += f"{','.join([str(self.short_ids[p_id]) for p_id in self.in_edges[node_id]])}"
            else:
                s += "."
            s += "/"
            if node_id in self.out_edges:
                s += f"{','.join([str(self.short_ids[c_id]) for c_id in self.out_edges[node_id]])}"
            else:
                s += "."
            s += "] "

        s += "\nroots: " 
        for root_id in self.roots:
            s += f"{self.short_ids[root_id]} "

        s += "\nedges: \n"
        for node_id, child_id in self.out_edges.items():
            for child in child_id:
                s += f"\t{self.short_ids[node_id]} -> {self.short_ids[child]}\n"

        s += "leaves: " 
        for leaf_id in self.leaves:
            s += f"{self.short_ids[leaf_id]} "
        return s

    
    def print(self) -> None:
        """ Print graph information. """
        print()
        print(self)
        print()


class OuterGraph(BaseGraph):
    
    # def __init__(self):
        # pass

    # def add_tool(self, tool: BaseCommandLineTool)

    def merge(
            self,
            node_a: OuterNode,
            node_b: OuterNode,
        ) -> str:
        raise NotImplementedError()
        return "new_outer_node_id"


class InnerGraph(BaseGraph):
    node_parents: List  # TODO specify type
    
    def __init__(self):
        pass


    def merge_with(
            self,
            other: InnerGraph
        ) -> InnerGraph:
        raise NotImplementedError()
        return "new_inner_graph_id"


# #########################################
# #                 Node                  #
# #########################################
# class Node:
#     def __init__(
#             self,
#             id: str,
#             processes: List[BaseProcess],
#             parents: Optional[List[str]] = None,    #List[parent_ids]
#             children: Optional[List[str]] = None,   #List[child_ids]
#             # internal_dependencies and graph are used in graph optimization 
#             is_tool_node: bool = False,
#             internal_dependencies: Optional[Dict[str, str | List[str]]] = None,  #{node_id: node_id}
#             graph: Optional['Graph'] = None
#         ) -> None:
#         """
#         Node containing one or more processes. Stores dependencies between a
#         node and its parents/children and between the processes in 
#         self.processes.
#         """
#         self.id = id
#         self.is_tool_node: bool = is_tool_node
#         self.processes: List[BaseProcess] = processes

#         if parents is None:
#             parents = []
#         self.parents: List[str] = parents

#         if children is None:
#             children = []
#         self.children: List[str] = children

#         if not is_tool_node:
#             if graph is None:
#                 if internal_dependencies is None:
#                     internal_dependencies = {}

#                 graph = self.create_task_graph(
#                     internal_dependencies,
#                     processes
#                 )
#         self.graph: Graph | None = graph


#     def __deepcopy__(self, memo) -> 'Node':
#         """
#         Make a deep copy of the node and return it.
#         NOTE: Creates copies are reference to the underlying processes,
#         instead of creating new processes. This ommits initializing
#         processes again, which is pointless and takes time.
#         """
#         # NOTE Untested
#         processes = self.processes.copy() # << Shallow copy!
#         # processes = [p for p in self.processes] # << Shallow copy!
#         parents = deepcopy(self.parents)
#         children = deepcopy(self.children)
#         graph = deepcopy(self.graph)
#         node = Node(
#             self.id, 
#             processes = processes, 
#             parents = parents, 
#             children = children,
#             is_tool_node = self.is_tool_node,
#             graph = graph
#         )
#         return node
    

#     def create_task_graph(
#             self,
#             dependencies: Dict[str, Union[str, List[str]]],
#             processes: List[BaseProcess]
#         ) -> 'Graph':
#         """
#         Arguments:
#             dependencies: Maps parent IDs to child IDs.
#             processes: Processes to be contained by nodes. 

#         Returns:
#             Task graph with nodes that hold a single CommandLineTool.
#         """
#         graph = Graph()

#         # Add nodes, but don't connect them yet
#         for process in processes:
#             graph.add_node(
#                 Node(
#                     process.id, 
#                     processes = [process], 
#                     is_tool_node = True
#                 )
#             )

#         # Connect nodes
#         for node_id, child_ids in dependencies.items():
#             if isinstance(child_ids, str):
#                 child_ids = [child_ids]
#             elif not isinstance(child_ids, list):
#                 raise Exception(f"Expected str or list, but found {type(child_ids)}")
#             graph.connect_node(node_id, children=child_ids)
#         return graph
    

#     def merge(
#             self,
#             nodes: Union['Node', List['Node']]
#         ) -> 'Node':
#         # self.merged = True
#         # 
#         raise NotImplementedError()

    
#     def is_leaf(self) -> bool:
#         return len(self.children) == 0
    
#     def is_root(self) -> bool:
#         return len(self.parents) == 0



# #########################################
# #                 Graph                 #
# #########################################
# class Graph:
#     def __init__(
#             self, 
#             # grouping: bool = False
#         ) -> None:
#         """
#         BUG where a node is added twice to parents/children
#         Directed Acyclic Graph (DAG) implementation to represent a workflow
#         task graph. Can be used to optimize task graph execution.
#         """
#         self.roots: List[str] = []  # [node_ids]
#         self.leaves: List[str] = [] # [node_ids]
#         self.nodes: Dict[str, Node] = {}    # {node_id, Node}
#         self.in_deps: Dict[str, List[str]] = {}  # {node_id: [parent_ids]}
#         self.out_deps: Dict[str, List[str]] = {}  # {node_id: [child_ids]}
#         self.size: int = 0
#         # self.grouping: bool = grouping
        
#         # Create placeholder IDs for nodes to improve readability
#         self._next_id: int = 0
#         self.short_id: Dict[str, int] = {}

    
#     def __deepcopy__(self, memo) -> 'Graph':
#         """set_groupings
#         Make a deep copy of the graph and return it.
#         NOTE: Creates shallow copies of nodes instead. This ommits initializing
#         processes again, which is pointless and takes time.
#         """
#         graph = Graph()
#         graph.roots = self.roots.copy()
#         graph.leaves = self.leaves.copy()
#         graph.nodes = self.nodes.copy() # << processes in nodes are references to originals!
#         graph.in_deps = self.in_deps.copy()
#         graph.out_deps = self.out_deps.copy()
#         graph._next_id = self._next_id
#         graph.short_id = self.short_id.copy()    # {node_id: simple_id}
#         # graph.roots = deepcopy(self.roots)
#         # graph.leaves = deepcopy(self.leaves)
#         # graph.nodes = deepcopy(self.nodes) # << processes in nodes are references to originals!
#         # graph.in_deps = deepcopy(self.in_deps)
#         # graph.out_deps = deepcopy(self.out_deps)
#         # graph._next_id = self._next_id
#         # graph.short_id = deepcopy(self.short_id)    # {node_id: simple_id}
#         return graph

    
    # def __str__(self) -> str:
    #     """
    #     Construct a string containing graph info and return it. Simple node
    #     IDs are used to improve clarity. Node IDs are mapped to simple node
    #     IDs in self.short_id.

    #     Returns a string containing:
    #         - Root nodes
    #         - Edges
    #         - Leaf nodes
    #     """
    #     s = "nodes[parents/children]: "
    #     for node_id in self.nodes:
    #         s += f"{self.short_id[node_id]}["
    #         if node_id in self.in_deps:
    #             s += f"{','.join([str(self.short_id[p_id]) for p_id in self.in_deps[node_id]])}"
    #         else:
    #             s += "."
    #         s += "/"
    #         if node_id in self.out_deps:
    #             s += f"{','.join([str(self.short_id[c_id]) for c_id in self.out_deps[node_id]])}"
    #         else:
    #             s += "."
    #         s += "] "
    #     s += "\nroots: " 
    #     for root_id in self.roots:
    #         s += f"{self.short_id[root_id]} "
    #     s += "\nedges: \n"
    #     for node_id, child_id in self.out_deps.items():
    #         for child in child_id:
    #             s += f"\t{self.short_id[node_id]} -> {self.short_id[child]}\n"
    #     s += "leaves: " 
    #     for leaf_id in self.leaves:
    #         s += f"{self.short_id[leaf_id]} "
    #     return s

    
#     def print(self) -> None:
#         """ Print graph information. """
#         print()
#         print(self)
#         print()

    
#     def next_id(self) -> int:
#         """ Generate and update the next available simple node ID. """
#         self._next_id += 1
#         return self._next_id - 1
    

#     def add_node(
#             self,
#             node: Node
#         ) -> None:
#         """
#         Add a node to the graph, without adding edges to other nodes.
#         NOTE: For connecting nodes with edges, see connect_node().
#         """
#         if node.id in self.nodes:
#             raise Exception(f"Node ID already exists in graph. Invalid ID: {node.id}")
        
#         self.nodes[node.id] = node
#         self.roots.append(node.id)
#         self.leaves.append(node.id)
#         self.short_id[node.id] = self.next_id()
#         self.size += 1


#     def connect_node(
#             self,
#             node_id: str,
#             parents: Optional[List[str]] = None,
#             children: Optional[List[str]] = None
#         ) -> None:
#         """
#         Add edges between a node and its parents/children.

#         Arguments:
#             node_id: ID of the node.
#             parents: List containing node IDs of parent nodes to connect to.
#             children: List containing node IDs of child nodes to connect to.
#         """
#         if node_id not in self.nodes:
#             raise Exception(f"Graph does not contain node with ID {node_id}")

#         node = self.nodes[node_id]

#         if parents is None:
#             parents = []
#         node.parents.extend(parents)
#         node.parents = list(set(node.parents))   # Removes duplicates

#         if children is None:
#             children = []
#         node.children.extend(children)
#         node.children = list(set(node.children)) # Removes duplicates

#         for parent_id in node.parents:
#             # Add node as child to parent
#             self.nodes[parent_id].children.append(node.id)
            
#             # Register in-dependencies
#             if node.id in self.in_deps:
#                 self.in_deps[node.id].append(parent_id)
#             else:
#                 self.in_deps[node.id] = [parent_id]
            
#             # Register out-dependencies for parent
#             if parent_id in self.out_deps:
#                 self.out_deps[parent_id].append(node.id)
#             else:
#                 self.out_deps[parent_id] = [node.id]

#             # Parent cannot be a leaf
#             if parent_id in self.leaves:
#                 self.leaves.remove(parent_id)

#         for child_id in node.children:
#             # Add node as parent to child
#             self.nodes[child_id].parents.append(node.id)
            
#             # Register in-dependencies for child
#             if child_id in self.in_deps:
#                 if not node.id in self.in_deps[child_id]:
#                     self.in_deps[child_id].append(node.id)
#             else:
#                 self.in_deps[child_id] = [node.id]
            
#             # Register out-dependencies
#             if node.id in self.out_deps:
#                 if not child_id in self.out_deps[node.id]:
#                     self.out_deps[node.id].append(child_id)
#             else:
#                 self.out_deps[node.id] = [child_id]

#             # child cannot be a root
#             if child_id in self.roots:
#                 self.roots.remove(child_id)

#         # Update graph root dict
#         if node.is_root():
#             if node.id not in self.roots:
#                 self.roots.append(node.id)
#         elif node.id in self.roots:
#                 self.roots.remove(node.id)

#         # Update graph leaf dict
#         if node.is_leaf():
#             if node.id not in self.leaves:
#                 self.leaves.append(node.id)
#         elif node.id in self.leaves:
#             self.leaves.remove(node.id)

    
#     def remove_node(
#             self,
#             node_id: str
#         ) -> None:
#         """
#         TODO Description
#         Remove a node
#         """
#         self.roots.remove(node_id)
#         self.leaves.remove(node_id)
#         self.nodes.pop(node_id)
#         self.size -= 1


#     def merge(
#             self,
#             node_ids: str | List[str]
#         ) -> Node:
#         """
#         TODO Description
#         Create a new Node in which nodes of ``node_ids`` are merged. The new
#         Node is added to the graph and a reference to the newly created node is
#         returned.
#         """
#         if isinstance(node_ids, str):
#             node_ids = [node_ids]
            
#         # Create new_node with id = {short_id0}:{short_id1}:...
#         new_id: str = ":".join([str(self.short_id[node_id]) for node_id in node_ids])
#         new_node = Node(new_id, [], is_tool_node = False)
#         self.add_node(new_node)

#         # Merge nodes into new node
#         new_node.merge([self.nodes[node_id] for node_id in node_ids])

#         # Remove old nodes
#         for node_id in node_ids:
#             self.remove_node(node_id)
#         return new_node
    

#     def get_nodes(
#             self,
#             node_ids: str | List[str]
#         ) -> List[Node]:
#         """
#         TODO
#         """
#         if isinstance(node_ids, str):
#             node_ids = [node_ids]
#         elif not isinstance(node_ids, list):
#             raise Exception(f"Expected str or list type, but got {type(node_ids)}")

#         return [self.nodes[node_id] for node_id in node_ids]