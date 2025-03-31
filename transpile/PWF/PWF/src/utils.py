from typing import Any, Optional, Tuple
from .Process import BaseProcess
# from pathlib import Path


class Absent:
    """ 
    Used to indictate the absence of a runtime input. Useful when None is a
    valid value for an argument.
    """
    pass


# def get_or_default(d: dict, default_value: Any) -> Any:
#     if not isinstance(d, dict):
#         raise TypeError(f"Expected dict, but has type {type(d)}")
#     if default_value in d:
#         return d[default_value]
#     return default_value

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

        if parents is None:
            parents = []
        self.parents = parents

        if children is None:
            children = []
        self.children = children

        if processes is None:
            processes = []
        self.procs = processes

        if internal_dependencies is None:
            internal_dependencies = {}
        self.internal_deps = internal_dependencies


    def merge(
            self,
            nodes: 'Node' | list['Node']
        ) -> 'Node':
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
            
