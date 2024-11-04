import typing
from pathlib import Path

import dask
import dask.delayed
# from dask.distributed import LocalCluster as Cluster
import graphviz         # Needed to visualize the graph


class Client:
    def __init__(self):
    # def __init__(self, TODO type_of_cluster):
        self.dependencies = None
        self.optimized = False
        self.process_clusters = None    # Grouped steps
        self.reader = Reader()
        self.runner = Runner()

        self.graph = None               # Will contain dependency graph


    def load_cwl_step(self):
        """ Use CWL step file instead of Process file. """
        # Useful for development until we use python files instead
        pass 


    def load_cwl_workflow(self):
        """ Use CWL workflow file instead of python workflow file. """
        # Useful for development until we use python files instead
        pass


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



