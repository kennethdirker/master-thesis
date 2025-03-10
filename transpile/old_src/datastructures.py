# Standard modules
from typing import Dict, List, Any, Union
from pathlib import Path

class DirType(Path):
    def __init__(self, *args, **kwargs):
        super.__init__(self, *args, **kwargs)
        self.path_type = "dir"

    def get_type(self):
        return self.path_type

class FileType(Path):
    def __init__(self, *args, **kwargs):
        super.__init__(self, *args, **kwargs)
        self.path_type = "file"

    def get_type(self):
        return self.path_type

InputTypes = Union[
    bool,
    str,
    int,
    float,
    DirType,
    FileType,
]

OutputTypes = Union[
    bool,
    str,
    int,
    float,
    DirType,
    FileType,
]

class Step:
    """"""
    ATTR_NAMES = [
        "cwlVersion",
        "id",
        "baseCommand",
        "arguments",
        "inputs",
        "outputs",
        "requirements",
        "hints",
        "stderr",
        "stdout",
        "stdin",
        "successCodes",
        "permanentFailCodes",
        "temporaryFailCodes",
        "save",
        "extension_fields",
        "intents"
        "label",
        "doc",
    ]

    def __init__(
            self,
            id: str,
            baseCommand: list[str],
            **kwargs: dict[str, Any]
            # inputs: dict[str, dict],
            # outputs: dict[str, dict]
        ):
        # self.cwlVersion
        self.attrs = []     # Keep track of the attributes assigned to the step
        self.id: str = id
        self.baseCommand: list[str] = baseCommand
        self.template = ""
        # self.arguments
        # self.inputs
        # self.outputs
        # self.requirements
        # self.hints
        # self.stderr
        # self.stdout
        # self.stdin
        # self.successCodes
        # self.permanentFailCodes
        # self.temporaryFailCodes
        # self.save
        # self.extension_fields
        # self.intents
        # self.label
        # self.doc
        self.add_attrs(kwargs)


    def add_attrs(self, **kwargs):
        """"""
        for attr, value in kwargs.items():
            if attr in ATTR_NAMES:
                setattr(self, attr, value)

                # Note down new attributes
                if attr not in self.attrs:
                    self.attrs.append(attr)
            else:
                # TODO Decide if debugging only
                raise Exception(f"{attr} is not a valid step attribute.")

    
    def create_template(self):
        self.template = ...


class Node:
    def __init__(
            self, 
            id: str,
            parents: list[str],
            steps: list[Step],
            is_grouped: bool = False,
            dependencies: dict[str, str] = {}
        ):
        self.id = id
        self.parents: list[str] = parents
        self.steps: list[Step] = steps

        # Used for indicating nested step dependencies
        self.is_grouped: bool = is_grouped
        self.dependencies: dict[int, int] = dependencies

class Graph:
    def __init__(self, grouping: bool = False):
        #TODO Which of the following 2?
        self.roots: list[Node] = []
        # self.roots: list[int] = []

        self.grouping: bool = grouping
        self.nodes: dict[int, Node] = {}
        self.dependencies = {}  # {child_id: [parent_ids], ...}




    def add_node(
            self,
            node: Node,
            parents: str | list[str] = None
        ):
        if parents is None:
            self.nodes[node.id] = node 

            #TODO Which of the following 2?
            self.roots.append(node)         # <-node reference
            # self.roots.append(node.id)    # <-node id
        else:
            if isinstance(parents, str):
                #TODO single parent
                if node.id in self.dependencies:
                    self.dependencies[node.id].append(parents)
                else:
                    self.dependencies[node.id] = [parents]

                # TODO WIP
                raise NotImplementedError()

                
                ...



            elif isinstance(parents, list):
                #TODO multiple parents
                # TODO WIP
                raise NotImplementedError()
            else:
                raise Exception(f"Invalid parameter type: parents({type(parents)}).")
