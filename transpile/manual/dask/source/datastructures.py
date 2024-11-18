# Standard modules
from typing import Dict, List


class Step:
    """"""
    ATTR_NAMES = [
        "id",
        "baseCommand",
        "requirements",
        "hints",
        "label",
        "stderr",
        "stdout",
        "stdin",
        "successCodes",
        "permanentFailCodes",
        "temporaryFailCodes",
        "save",
        "extension_fields",
        "intents"
    ]


    def add_attrs(self, **kwargs):
        """"""
        for attr, value in kwargs.items():
            if attr in ATTR_NAMES:
                setattr(self, attr, value)
            else:
                # TODO Decide if debugging only
                raise Exception(f"{attr} is not a valid step attribute.")

    def __init__(
            self,
            id: str,
            baseCommand: List[str],
            **kwargs
            # inputs: Dict[str, dict],
            # outputs: Dict[str, dict]
        ):
        self.id: str = id
        self.baseCommand: List[str] = baseCommand
        self.add_attrs(kwargs)
        # self.inputs: Dict[str, dict] = inputs
        # self.outputs: Dict[str, dict] = outputs
        # self.requirements
        # self.hints
        # self.label
        # self.stderr
        # self.stdout
        # self.stdin
        # self.successCodes
        # self.permanentFailCodes
        # self.temporaryFailCodes
        # self.save
        # self.extension_fields
        # self.intent


class Node:
    def __init__(
            self, 
            id: str,
            parents: List[str],
            steps: List[Step],
            is_grouped: bool = False,
            dependencies: Dict[str, str] = {}
        ):
        self.id = id
        self.parents: List[str] = parents
        self.steps: List[Step] = steps

        # Used for indicating nested step dependencies
        self.is_grouped: bool = is_grouped
        self.dependencies: Dict[int, int] = dependencies

class Graph:
    def __init__(self, grouping: bool = False):
        #TODO Which of the following 2?
        self.roots: List[Node] = []
        # self.roots: List[int] = []

        self.grouping: bool = grouping
        self.nodes: Dict[int, Node] = {}
        self.dependencies = {}  # {child_id: [parent_ids], ...}




    def add_node(
            self,
            node: Node,
            parents: str | List[str] = None
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

                
                ...



            elif isinstance(parents, list):
                #TODO multiple parents
                # TODO WIP
                pass
            else:
                raise Exception(f"Invalid parameter type: parents({type(parents)}).")
