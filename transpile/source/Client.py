# Standard modules
import typing
from pathlib import Path

# External modules
import dask
import dask.delayed
# from dask.distributed import LocalCluster as Cluster
import graphviz         # Needed to visualize the graph

# Local modules
import Reader, Runner
# import datastructures as ds

class Client:
    def __init__(self):
    # def __init__(self, TODO type_of_cluster):
        self.dependencies = None        # TODO: needed?
        self.optimized = False          # TODO: needed?
        self.process_clusters = None    # Grouped steps
        self.reader = Reader()
        self.runner = Runner()

        self.graph = None               # Will contain dependency graph
        self.cwl_object = None
        self.job_input = {}

    # def load_cwl_step(self):
    #     """ Use CWL step file instead of Python Process file. """
    #     # Useful for development until we use python files instead
    #     pass 


    # def load_cwl_workflow(self):
    #     """ Use CWL workflow file instead of Python Workflow file. """
    #     # Useful for development until we use python files instead
    #     pass


    def load_cwl_object(self, path: str | Path):
        """ Load a CWL object to be executed. """
        if isinstance(path, str):
            path = Path(path)
        if isinstance(path, Path):
            if not path.is_file():
                raise Exception(f"{path} is not a file")
            self.cwl_object = self.reader.load_cwl(path)
        else:
            raise Exception("'path' argument should be 'str' or 'Path'")


    def matching_inputs(self):
        """ 
        Check whether all parameters to execute the CWL object are matched
        with an argument in the input object.
        """
        for param in self.cwl_object:
            # TODO Deal with optional parameters
            if param not in self.job_input:
                return False
        return True
    

    def load_job_input(self, path: str | Path):
        """ Load CWL inputs to be bound to the command line. """
        if isinstance(path, str):
            path = Path(path)
        if isinstance(path, Path):
            if not path.is_file():
                raise Exception(f"{path} is not a file")
            self.job_input = self.reader.load_job_input(path)
            if not self.matching_inputs():
                raise Exception("Missing parameters in the input object")
        else:
            raise Exception("'path' argument should be 'str' or 'Path'")

    def load_python_workflow(self):
        pass 


    def load_python_step(self):
        # We want this to be able to test single steps implemented in
        # python files.
        pass


    def exec_task_graph(self):
        # Send dependency graph to the Runner, which creates a dask.Delayed
        # object. The dask.Delayed is then executed on the cluster.
        pass


    def group_steps(self):
        """ 
        Optimize the task graph by creating a task graph of grouped steps.
        A grouped step is a set of steps that can be executed in serial without
        the need of additional data from outside the container. NOTE: or node?
        """
        # Can be used as dataflow optimization
        pass


    def visualize_graph(self, save_path: Path = None):
        """ Save a visualization of the Dask task graph. """
        path = save_path if Path else "graph.svg"
        self.graph.visualize(filename=path)



