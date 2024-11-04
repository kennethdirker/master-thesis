import dask
import dask.delayed
from dask.distributed import LocalCluster as Cluster

import spython          # Singularity bindings


class Runner:
    def __init__(self):
    # def __init__(self, TODO type_of_cluster):
        # if type is distributed:
        #     from dask.distributed import DistributedCluster as Cluster
        # else
        #     from dask.distributed import LocalCluster as Cluster

        self.cluster = Cluster()
        self.client = self.cluster.get_client()   # Sets environment

        self.task_graph = None
    

    def exec_task_graph(self, graph):
        self.task_graph: dask.Delayed = None
        # How a Process is executed by the runner:
        # Start container if needed
        # Load input
        # Input manipulation with _in_script()
        # Command is executed by Runner
        # Output manipulation with _out_script()
        # Export output? NOTE: What if we use mounted volume?
        # Stop container if needed