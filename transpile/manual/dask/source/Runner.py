# Standard modules
import subprocess, time

# External modules
import dask
import dask.delayed
from dask.distributed import LocalCluster as Cluster
import spython          # Singularity bindings

# Local modules 
import datastructures as ds


class Runner:
    def __init__(self):
    # def __init__(self, TODO type_of_cluster):
        # if type is distributed:
        #     from dask.distributed import DistributedCluster as Cluster
        # else
        #     from dask.distributed import LocalCluster as Cluster

        self.cluster = Cluster()
        self.client = self.cluster.get_client()   # Sets environment

    @dask.delayed
    def _step_wrapper(
            self,
            node: ds.Node,
            inputs: dict [str, Any],
        ):
        """
        A wrapper around a step (or group of steps) that can be used in
        building a DASK Delayed task graph.
        """
        # How a Process is executed by the runner:
        # Start container if needed
        # Load input
        # Input manipulation with _in_script()
        # Command is executed by Runner
        # Output manipulation with _out_script()
        # Export output? NOTE: What if we use mounted volume?
        # Stop container if needed
        
        # TODO Start container

        
        for step in steps:
            # TODO Load input to right node. NOTE: for later 
            # TODO Chain grouped steps in single container
            
            # TODO Check if inputs are accessible 
            for key, binding in step.inputs.items():
                pass

            # Create command line
            template = step.template
            # TODO Fill in command line template
            cmd: list[str] = template.split(" ")

            # https://docs.python.org/3/library/subprocess.html
            completed_process = subprocess.run(
                cmd,
                capture_output = True,
                stderr = stderr, 
                stdout = stdout,
                stdin = stdin, 
            )

        # TODO Stop container
    ...


    def create_task_graph(self, graph: ds.Graph):
        task_graph: dask.Delayed = None

        # Walk tree

        return task_graph



    def exec_task_graph(
            self, 
            tasks: dask.Delayed,
            inputs: dict[str, Any], 
            verbose = False,
        ):
        start = time.time()
        result = tasks.compute()
        end = time.time()
        time_taken = end - start 
        if verbose:
            print(f"Task graph executed in {time_taken}.")
        return {"result": result, "time": time_taken}



