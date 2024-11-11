# Standard modules
from pathlib import Path
from typing import Any, Optional, List, Dict

# External modules
# import ruamel.yaml
from cwl_utils.parser import load_document_by_uri, cwl_version

# Local modules 
import Graph
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


    def load_cwl_step(self, path: Path) -> DependencyNode:        
        # Load CWL step file into an object 
        if not path.is_file():
            raise Exception(f"{path} is not a file.")
        if cwl_version(path) not in "v1.2":
            raise Exception(f"CWL version of file {path} is not v1.2.")
        cwl_object = load_document_by_uri(path)

        class_ = retrieve_attr(cwl_object, "class_")
        if class_ not in "CommandLineTool":
            raise Exception(f"File at path {path} not a command line tool.")

        step_node = DependencyNode()
        id = retrieve_attr(cwl_object, "id")
        baseCommand = retrieve_attr(cwl_object, "baseCommand")
        inputs = retrieve_attr(cwl_object, "inputs")
        outputs = retrieve_attr(cwl_object, "outputs")
        parents = ...
        #TODO WIP
        return step_node

    
    def load_cwl_workflow(self):
        pass


    def load_cwl(self, path: Path) -> DependencyGraph:
        # Load CWL step file into an object 
        if not path.is_file():
            raise Exception(f"{path} is not a file.")
        if cwl_version(path) not in "v1.2":
            raise Exception(f"CWL version of file {path} is not v1.2.")
        cwl_object = load_document_by_uri(path)

        graph = DependencyGraph()
        file_class = retrieve_attr(cwl_object, "class_")
        if file_class in "CommandLineTool":
            # Add node to graph
            node = self.load_cwl_step(path)
            graph.add_node(node, parents)
        elif file_class in "workflow":
            #TODO Breadth-first walk CWL steps/workflows
            pass    #TODO
        elif file_class in "ExpressionTool":
            pass    #TODO
        else:
            raise Exception(f"CWL tool type invalid.")
        return graph


    def load_python_step(self):
        pass


    def load_python_workflow(self):
        pass



    # def __init__(self, cwl_file: Path) -> object:
    #     """
    #     """
        # # TODO Single step
        # # TODO Flat workflow
        # # TODO Nested workflow

        # self.path = Path(workflow_path)     # Path of first file
        # if not self.path.is_file():
        #     raise Exception(f"Path '{workflow_path}' does not exist.")
        
        # if input_yaml_path:        
        #     self.inputs = Path(input_yaml_path) # Path to yaml input
        #     if not self.inputs.is_file():
        #         raise Exception(f"Path '{input_yaml_path}' does not exist.")
        
        # # Read input workflow file, raises exception on invalid CWL files
        # self.cwl_obj = load_document_by_uri(self.path)

        # # Read input yaml
        # if self.inputs:
        #     self.inputs = yaml.safe_load(self.inputs.read_text())
        #     if not self.inputs:
        #         raise Exception(f"'{input_yaml_path}' is not valid YAML.")
        

        # # build task graph
        # if self.cwl_obj.class_ == "CommandLineTool":
        #     if type(self.cwl_obj.baseCommand) is str:
        #         cmd = [self.cwl_obj.baseCommand]
        #     elif type(self.cwl_obj.baseCommand) is list:
        #         cmd = self.cwl_obj.baseCommand
            
        #     # Add input arguments to command line
        #     if self.inputs:
        #         for input in self.cwl_obj.inputs:
        #             # input: cwl_utils.parser.cwl_v1_2.CommandInputParameter
        #             begin = input.id.rfind('#') + 1
        #             end = len(input.id)
        #             spliced_id = input.id[begin:end]

        #             # Throw exception if the *required* input argument 
        #             # is not in input YAML
        #             if not self.inputs[spliced_id]:
        #                 raise Exception(f"Required input argument '{spliced_id}' (from '{input.id}') is not in input YAML.")

        #             cmd.append(self.inputs.get(spliced_id))

        #     # Create DASK task graph holding function call
        #     print(cmd)
        #     self.graph = dask.delayed(sp.call(cmd , shell=True))

        # elif self.cwl_obj.class_ == "ExpressionTool":
        #     pass
        # elif self.cwl_obj.class_ == "Workflow":
        #     pass
        # else:
        #     raise Exception("Unknown CWL class type.")

        # # self.tasks = []     # Holds 
        # # self.graph: dask.Delayed = None        

