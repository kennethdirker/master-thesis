import pathlib

# import ruamel.yaml
from cwl_utils.parser import load_document_by_uri       #cwl2object parser

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

    def load_cwl_step(self):
        pass
    
    def load_cwl_workflow(self):
        pass

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

