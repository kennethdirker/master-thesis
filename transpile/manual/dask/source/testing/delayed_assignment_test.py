import sys 
import dask
import dask.delayed
from typing import List, Any, Dict
import time

# from collections import deque

import dask.delayed
import graphviz             # To visualize the graph
from string import Template # To create shell command line
import copy                 # To make deep copy of dict
import subprocess           # Execute shell command line 


# class CList:
#     """ 
#     Cyclic list implementation.
#     Performance should be fine if the list size stays small. 
#     """
#     def __init__(
#             self,
#             values: list[Any] = None
#         ):
#         if values:
#             self._values = values 
#         else:
#             self._values = []
#         self.index = 0


#     def size(self) -> int:
#         """ Return the amount of elements in the cyclic list. """
#         return len(self._values)


#     def next(self) -> Any:
#         if self.is_empty():
#             raise Exception("Clist is empty")
#         self.index = (self.index + 1) % self.size()

    
#     def peak(self) -> Any:
#         """ Return the element at the current index. """
#         return self._values[self.index]


#     def add(self, elem: Any) -> None:
#         """ Add new element before the clist index. """
#         if self.is_empty():
#             self._values.append(elem)
#             self.index = 0

#         self._values.insert(self.index, elem)
#         self.index += 1

    
#     def delete(self, index: int) -> Any:
#         """ Delete the element at a given index. Returns the removed value. """
#         if type(index) != type(int):
#             raise Exception("Index type not an integer")

#         if index > self.size() - 1 or index < 0:
#             raise Exception("Index out of bounds")

#         if self.is_empty():
#             raise Exception("Clist is empty")

#         # If element to remove is before clist index, decrement clist index 
#         if index < self.index:
#             self.index -= 1

#         # If element to remove is tail, while the clist index points at tail,
#         # cycle the clist index back to head (0)
#         if index == self.index and index == self.size() - 1:
#             self.index = 0

#         return self._values.pop(index)


#     def is_empty(self):
#         """ Returns a boolean base"""
#         return len(self._values) == 0


#     def empty(self):
#         """ Clear all elements from the cyclic list. """
#         self._values.clear()
#         self.index = 0



class Node:
    id: int = 0

    def __init__(self, parents, children, value) -> None:
        self.parents: int = parents
        self.children: int = children
        self.value: Any = value
        self.id: int = self.new_id()

    def new_id(self) -> int:
        ret = id 
        self.id += 1
        return ret

    def is_leaf(self) -> bool:
        return len(self.children) == 0
    
    def is_root(self) -> bool:
        return len(self.parents) == 0


class Graph:
    def __init__(self):
        self.roots: dict[int, Node]  = {}
        self.nodes: dict[int, Node] = {}
        self.in_edges: dict[int, list[int]]  = {}    # parents of nodes
        self.out_edges: dict[int, list[int]] = {}   # children of nodes

    def add_node(self, node: Node):
        for parent in node.parents:
            if node.id in self.in_edges:
                self.in_edges[node.id].append(parent)
            else:
                self.in_edges[node.id] = [parent]
        
        for child in node.children:
            if node.id in self.in_edges:
                self.in_edges[node.id].append(child)
            else:
                self.in_edges[node.id] = [child]
        
        if node.is_root():
            self.roots[node.id] = node
        self.nodes[node.id] = node


class NoneType(object):
    """ Indicates incorrect / non-existent data. """
    def __str__(self):
        return "NoneType"
    def __repr__(self):
        return "NoneType"


class State:
    """ Dictionary wrapper """
    def __init__(self, d: dict = None):
        if d:
            self.state = copy.deepcopy(d)
        else:
            self.state = {}

    def contains(self, key):
        return key in self.state
    
    def get(self, key):
        """ 
        Returns the value paired to the given key.
        If the dictionary does not contain the key, raise a KeyError exception.
        """
        if key in self.state:
            return self.state[key]
        # return NoneStateType 
        raise KeyError
    
    def set(self, key, value):
        self.state[key] = value
        



# @dask.delayed
def wrapper(self, node, state: dict):
    cmd = node.value    # <-- command line template
    # If all inputs are missing in state dict, throw error
    try:
        value = state.get("value")
        cmd.substitute(value=value)
    except KeyError as e:
        print("Command line parameter not filled: ", e)
    except ValueError as e:
        print("Invalid command line template argument name: ", e)

    subprocess.run(cmd)
    return value + 1


def build_graph(graph: Graph, inputs: dict) -> dask.Delayed:
    task_graph = None
    nodes = graph.nodes
    node_results = {}
    for node in nodes:
        # Template for shell command to be executed by wrapper function
        cmd_template = node.value

        # Prepare input state entries with empty values
        input_keys = cmd_template.get_identifiers()
        for key in input_keys:
            inputs[key] = NoneType()

        # Prepare node bindings 
        # node_results[node.id] = NoneType()

    # Add nodes to taskgraph
    for node in nodes:
        node_results[node.id] = dask.delayed(wrapper)(node, inputs)

            
    return task_graph


def main():
    cmd_template = Template("echo $value")
    nodes = []        
    # nodes.append(Node([], [4], cmd_template))
    # nodes.append(Node([], [3], cmd_template))
    # nodes.append(Node([2], [4, 6], cmd_template))
    # nodes.append(Node([1,3], [5], cmd_template))
    # nodes.append(Node([4], [9], cmd_template))
    # nodes.append(Node([3], [7, 9], cmd_template))
    # nodes.append(Node([6], [8], cmd_template))
    # nodes.append(Node([7], [10], cmd_template))
    # nodes.append(Node([5, 6], [10], cmd_template))
    # nodes.append(Node([8, 9], [], cmd_template))
    nodes.append(Node([], [0], cmd_template))
    nodes.append(Node([0], [1], cmd_template))
    nodes.append(Node([1], [2], cmd_template))
    nodes.append(Node([2], [3], cmd_template))
    nodes.append(Node([3], [4], cmd_template))
    nodes.append(Node([4], [5], cmd_template))
    nodes.append(Node([5], [6], cmd_template))
    nodes.append(Node([6], [7], cmd_template))
    nodes.append(Node([7], [8], cmd_template))
    nodes.append(Node([8], [], cmd_template))
    graph = Graph()
    for node in nodes:
        graph.add_node(node)

    # state = State()
    state = {}
    task_graph = build_graph(graph, state)
    task_graph.visualize()
    res = task_graph.compute()
    print(res)

if __name__ == "__main__":
    main()