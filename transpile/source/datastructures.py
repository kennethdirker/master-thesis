# Standard modules
from typing import Dict, List, Any, Union
from pathlib import Path
import copy
import cwl_utils.parser.cwl_v1_2 as cwl

# TODO change to use cwl_utils
class DirType(Path):
    def __init__(self, *args, **kwargs):
        super.__init__(self, *args, **kwargs)
        self.path_type = "dir"

    def get_type(self):
        return self.path_type

# TODO change to use cwl_utils
class FileType(Path):
    def __init__(self, *args, **kwargs):
        super.__init__(self, *args, **kwargs)
        self.path_type = "file"

    def get_type(self):
        return self.path_type

# TODO change to use cwl_utils
InputTypes = Union[
    bool,
    str,
    int,
    float,
    DirType,
    FileType,
]

# TODO change to use cwl_utils
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
    # TODO Turn into Enum? Example:
    # from enum import Enum
    # class StepAttrs(Enum):
    #     cwlVersion = 1
    #     id = 2
    #     baseCommand = 3
    
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
            tool_id: str,
            # NOTE: baseCommand is optional and contain empty list, does this happen in LINQ? 
            baseCommand: list[str],
            inputs: list[cwl.CommandInputParameter],
            outputs: list[cwl.CommandOutputParameter],
            **kwargs: dict[str, Any]
        ):
        # self.cwlVersion
        self.attrs: list[str] = []     # Keep track of the attributes assigned to the step
        self.id: str = tool_id
        # NOTE: baseCommand is optional, does this happen in LINQ? 
        self.baseCommand: list[str] = baseCommand
        # self.baseCommand: list[str] | str = baseCommand
        self.template = ""  #NOTE: Will hold template used to substitute step inputs
        # self.arguments
        self.inputs = inputs
        self.outputs = outputs
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
            if attr in self.ATTR_NAMES:
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
            # is_grouped: bool = False,
            dependencies: dict[str, str] | None = None
        ):
        self.id = id

        # NOTE: Do these assignments need deepcopies???
        self.parents: list[str] = parents
        self.steps: list[Step] = steps

        # Used for indicating nested step dependencies
        # self.is_grouped: bool = is_grouped
        if dependencies:
            # NOTE: Does this assignment need deepcopy???
            dependencies_copy = copy.deepcopy(dependencies)
            self.dependencies: dict[int, int] = dependencies_copy
        else:
            self.dependencies = None

class Graph:
    def __init__(
            self, 
            # grouping: bool = False
        ):
        #TODO Which of the following 2?
        self.roots: list[Node] = []
        # self.roots: list[int] = []

        # self.grouping: bool = grouping
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
