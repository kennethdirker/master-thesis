# Standard modules
from pathlib import Path
from typing import Any, Optional, List, Dict, Union

# External modules
# import ruamel.yaml
from cwl_utils.parser import load_document_by_uri, cwl_version

# Local modules 
# import datastructures as ds
from Datastructures import Step, Node, Graph
from Utils import getattr_or_nonetype, NoneType


class Reader:
    """
    Reading of CWL files
    Creating of dependency graph
    """
    # Reader workflow:
    # 1. Read CWL file and convert to Python object.
    # 2. Validate the CWL file (CWL-utils does this while reading in step 1).
    # 3. Create dependency graph with Process objects as nodes. 
    # 4. Return dependency graph.

    def __init__(self, starting_id: int = 0):
        self._next_id = starting_id     # Used to assign IDs to graph nodes


    def _new_node_id(self) -> int:
        id = self._next_id
        self._next_id += 1
        return id
    

    def load_step_node(self, path: Path) -> Node:
        """
        TODO: Test        
        """
        # Load CWL step file into an object with cwl-utils
        if not path.is_file():
            raise Exception(f"{path} is not a file.")
        cwl_obj = load_document_by_uri(path)
        if getattr(cwl_obj, "cwlVersion") not in "v1.2":
            raise Exception(f"CWL version of file {path} is not v1.2.")

        # Check whether CWL file really is a CommandLineTool
        class_ = getattr(cwl_obj, "class_")
        if "CommandLineTool" not in class_:
            # TODO: Update exception type
            raise Exception(f"File at path {path} not a command line tool.")

        # Get required CWL fields
        # tool_id = getattr(cwl_obj, "id")
        # baseCommand = getattr(cwl_obj, "baseCommand")
        # inputs = getattr(cwl_obj, "inputs")
        # outputs = getattr(cwl_obj, "outputs")

        # Get optional CWL fields
        kwargs = {}
        for attr in cwl_obj.attrs:
            # cwl-utils uses 'class_' instead of 'class', 
            # because 'class' is a Python keyword and doesn't play nice,
            # TODO: Determine whether to do this in the Step class
            attr = "class_" if "class" in attr else attr
            kwargs[attr] = getattr_or_nonetype(cwl_obj, attr)

        step = Step(
            # id = tool_id,
            # baseCommand = baseCommand,
            # inputs = inputs,
            # outputs = outputs,
            **kwargs
        )

        return Node(
            node_id = self._new_node_id(),
            parents = [],
            steps = step
        )

    
    def load_workflow_graph(self):
        raise NotImplementedError()


    def load_cwl(self, path: Path) -> Graph:
        # Load CWL step file into an object 
        if not path.is_file():
            raise Exception(f"{path} is not a file.")
        cwl_object = load_document_by_uri(path)
        v = getattr(cwl_object, "cwlVersion")
        if v not in "v1.2":
            raise Exception(f"Wrong CWL version: {path} is not v1.2, but {v}.")

        graph = Graph()
        file_class = getattr(cwl_object, "class_")
        if file_class in "CommandLineTool":
            # Add node to graph
            node = self.load_step_node(path)
            graph.add_node(node)
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