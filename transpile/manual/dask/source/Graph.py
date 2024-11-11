# Standard modules
from typing import Dict, List

class Step:
    def __init__(
            self,
            id: str,
            baseCommand: List[str],
            inputs: Dict[str, dict],
            outputs: Dict[str, dict]
        ):
        self.id: int = id
        self.baseCommand: List[str] = baseCommand
        self.inputs: Dict[str, dict] = inputs
        self.outputs: Dict[str, dict] = outputs
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


class DependencyNode:
    def __init__(
            self, 
            id: str | int,
            parents: List[int],
            steps: List[Step],
            is_grouped: bool = False,
            dependencies: Dict[int, int] = {}
        ):
        self.id = id
        self.parents: List[int] = parents
        self.steps: List[Step] = steps

        # Used for indicating nested step dependencies
        self.is_grouped: bool = is_grouped
        self.dependencies: Dict[int, int] = dependencies

class DependencyGraph:
    def __init__(self, grouping: bool = False):
        self.grouping: bool = grouping
        self.nodes: Dict[int, DependencyNode] = {}

        #TODO Which of the following 2?
        self.roots: List[DependencyNode] = []
        # self.roots: List[int] = []

        self.dependencies = {}  # {child_id: [parent_ids], ...}


    def add_node(
            self,
            node: DependencyNode,
            parents: int | List[int] = None
        ):
        if parents is None:
            self.nodes[node.id] = node 

            #TODO Which of the following 2?
            self.roots.append(node)         # <-node reference
            # self.roots.append(node.id)    # <-node id
        else:
            if isinstance(parents, int):
                #TODO single parent
                if node.id in self.dependencies:
                    self.dependencies[node.id].append(parents)
                else:
                    self.dependencies[node.id] = [parents]
            elif isinstance(parents, list):
                #TODO multiple parents
                pass
            else:
                raise Exception(f"Invalid parameter type: parents({type(parents)}).")
