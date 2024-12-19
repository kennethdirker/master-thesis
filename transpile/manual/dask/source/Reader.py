# Standard modules
from pathlib import Path
from typing import Any, Optional, List, Dict

# External modules
# import ruamel.yaml
from cwl_utils.parser import load_document_by_uri, cwl_version

# Local modules 
import datastructures as ds
from Utils import retrieve_attr


class Reader:
    """
    Reading of CWL files
    Creating of dependency graph
    """
    # Reader workflow:
    # Read workflow file 
    # Create dependency graph with Process objects as nodes. 
    # At the same time: Validate the workflow  
    # The dependency graph is returned.

    def __init__(self):
        self._new_id = 0     # Used to assign IDs to graph nodes


    def _create_node_id(self) -> int:
        id = self._new_id
        self._new_id += 1
        return id


    def load_cwl_step(self, path: Path) -> ds.Node:        
        # Load CWL step file into an object 
        if not path.is_file():
            raise Exception(f"{path} is not a file.")
        if cwl_version(path) not in "v1.2":
            raise Exception(f"CWL version of file {path} is not v1.2.")
        cwl_object = load_document_by_uri(path)

        # Check whether CWL file really is a CommandLineTool
        class_ = retrieve_attr(cwl_object, "class_")
        if class_ not in "CommandLineTool":
            raise Exception(f"File at path {path} not a command line tool.")

        # 
        step_node = ds.Node()
        id = retrieve_attr(cwl_object, "id")
        baseCommand = retrieve_attr(cwl_object, "baseCommand")
        inputs = retrieve_attr(cwl_object, "inputs")
        outputs = retrieve_attr(cwl_object, "outputs")
        parents = ...
        #TODO WIP
        return step_node

    
    def load_cwl_workflow(self):
        raise NotImplementedError()


    def load_cwl(self, path: Path) -> ds.Graph:
        # Load CWL step file into an object 
        if not path.is_file():
            raise Exception(f"{path} is not a file.")
        v = cwl_version(path)
        if v not in "v1.2":
            raise Exception(f"Wrong CWL version: {path} is not v1.2, but {v}.")
        cwl_object = load_document_by_uri(path)

        graph = ds.Graph()
        file_class = retrieve_attr(cwl_object, "class_")
        if file_class in "CommandLineTool":
            # Add node to graph
            node = self.load_cwl_step(path)
            graph.add_node(node, parents)
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