# Standard modules
from pathlib import Path
from typing import Any, Optional, List, Dict

# External modules
# import ruamel.yaml
from cwl_utils.parser import load_document_by_uri, cwl_version

# Local modules 
# import datastructures as ds
from datastructures import Step, Node, Graph
# from Utils import getattr_or_none, NoneType


class Reader:
    """
    Reading of CWL files
    Creating of dependency graph
    """
    # Reader workflow:
    # 1. Read workflow file and convert to Python object.
    # 2. Validate the workflow  
    # 3. Create dependency graph with Process objects as nodes. 
    # 4. Return dependency graph.

    def __init__(self, starting_id: int = 0):
        self._next_id = starting_id     # Used to assign IDs to graph nodes


    def _new_node_id(self) -> int:
        id = self._next_id
        self._next_id += 1
        return id


    def load_step_node(self, path: Path) -> Node:        
        # Load CWL step file into an object 
        if not path.is_file():
            raise Exception(f"{path} is not a file.")
        if cwl_version(path) not in "v1.2":
            raise Exception(f"CWL version of file {path} is not v1.2.")
        cwl_object = load_document_by_uri(path)

        # Check whether CWL file really is a CommandLineTool
        class_ = getattr(cwl_object, "class_")
        if "CommandLineTool" not in class_:
            # TODO: Update exception type
            raise Exception(f"File at path {path} not a command line tool.")

        tool_id = getattr(cwl_object, "id")
        baseCommand = getattr(cwl_object, "baseCommand")
        inputs = getattr(cwl_object, "inputs")
        outputs = getattr(cwl_object, "outputs")
        parents = ...

        step = Step(
            tool_id = tool_id,
            baseCommand = baseCommand,
            # TODO wip
        )

        return Node(
            id = self._new_node_id(),
            parents = parents,
            steps = [step]
        )

    
    def load_workflow_graph(self):
        raise NotImplementedError()


    def load_cwl(self, path: Path) -> Graph:
        # Load CWL step file into an object 
        if not path.is_file():
            raise Exception(f"{path} is not a file.")
        v = cwl_version(path)
        if v not in "v1.2":
            raise Exception(f"Wrong CWL version: {path} is not v1.2, but {v}.")
        cwl_object = load_document_by_uri(path)

        graph = Graph()
        file_class = getattr(cwl_object, "class_")
        if file_class in "CommandLineTool":
            # Add node to graph
            node = self.load_step_node(path)
            graph.add_node(node, path)
        elif file_class in "workflow":
            #TODO Breadth-first walk CWL steps/workflows
            raise NotImplementedError()
        elif file_class in "ExpressionTool":
            raise NotImplementedError()
        else:
            raise Exception(f"CWL tool type invalid.")
        return graph


    def load_python_step(self):
        raise NotImplementedError()


    def load_python_workflow(self):
        raise NotImplementedError()


    def load_input_yaml(self):
        # TODO can probably be done by cwl-utils?
        raise NotImplementedError()