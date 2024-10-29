import dask
import dask.delayed
import dask.delayed
import graphviz
from dask.distributed import LocalCluster as Cluster
from cwl_utils.parser import load_document_by_uri       #cwl2object parser
import argparse
# from typing import Any
from pathlib import Path
import subprocess as sp
import yaml
from queue import LifoQueue as Q

program_name = "Workflow Runner"



class Runner:
    def __init__(self, workflow_path: str, input_yaml_path: str = None):
        """
        """
        # TODO Single step
        # TODO Flat workflow
        # TODO Nested workflow

        self.path = Path(workflow_path)     # Path of first file
        if not self.path.is_file():
            raise Exception(f"Path '{workflow_path}' does not exist.")
        
        if input_yaml_path:        
            self.inputs = Path(input_yaml_path) # Path to yaml input
            if not self.inputs.is_file():
                raise Exception(f"Path '{input_yaml_path}' does not exist.")
        
        # Read input workflow file, raises exception on invalid CWL files
        self.cwl_obj = load_document_by_uri(self.path)

        # Read input yaml
        if self.inputs:
            self.inputs = yaml.safe_load(self.inputs.read_text())
            if not self.inputs:
                raise Exception(f"'{input_yaml_path}' is not valid YAML.")
        

        # build task graph
        if self.cwl_obj.class_ == "CommandLineTool":
            if type(self.cwl_obj.baseCommand) is str:
                cmd = [self.cwl_obj.baseCommand]
            elif type(self.cwl_obj.baseCommand) is list:
                cmd = self.cwl_obj.baseCommand
            
            # Add input arguments to command line
            if self.inputs:
                for input in self.cwl_obj.inputs:
                    # input: cwl_utils.parser.cwl_v1_2.CommandInputParameter
                    begin = input.id.rfind('#') + 1
                    end = len(input.id)
                    spliced_id = input.id[begin:end]

                    # Throw exception if the *required* input argument 
                    # is not in input YAML
                    if not self.inputs[spliced_id]:
                        raise Exception(f"Required input argument '{spliced_id}' (from '{input.id}') is not in input YAML.")

                    cmd.append(self.inputs.get(spliced_id))

            # Create DASK task graph holding function call
            print(cmd)
            self.graph = dask.delayed(sp.call(cmd , shell=True))

        elif self.cwl_obj.class_ == "ExpressionTool":
            pass
        elif self.cwl_obj.class_ == "Workflow":
            pass
        else:
            raise Exception("Unknown CWL class type.")

        # self.tasks = []     # Holds 
        # self.graph: dask.Delayed = None        

        # Set cluster environment (local at the moment)
        try:
            self.cluster = Cluster()    # Cluster to execute workflow on
            self.client = self.cluster.get_client()   # Sets environment
        except:
            raise Exception("Initialisation of cluster failed.")


    def gather_tools(self):
        """
        Create the structures required to chain steps.
        """



    def build_task_graph(self):
        """ 
        Compose the steps of the workflow in a dask.delayed object. 
        """        
        

    def execute_graph(self):
        """
        
        """
        self.graph.compute()

    def visualize_graph(self):
        self.graph.visualize()
        

    def _execute_step(self):
        """
        
        """


def main(cwl_file: str, inputs_file: str):
    """
    """
    runner = Runner(cwl_file, inputs_file)
    runner.visualize_graph()
    runner.execute_graph()

    # Command line tool usage in workflow:
    # TODO Check workflow
    # TODO Load workflow
    # TODO Create Taskgraph
    # TODO Execute Taskgraph
    # TODO Write workflow output

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog=program_name,
                    description="Execute a cwl workflow with DASK.",
                    epilog="")
    parser.add_argument("-f", "--file", required=True)
    parser.add_argument("-i", "--input", required=False)
    args = parser.parse_args()

    inputs = args.input if args.input else None

    if args.file:
        main(args.file, inputs)
    else:
        raise Exception("No workflow file given. Use '-f' or '--file' argument!")