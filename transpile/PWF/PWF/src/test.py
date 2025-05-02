from dask.distributed import Client
from subprocess import run, CompletedProcess
from copy import deepcopy
from typing import Any

class Command:
    def __init__(self, cmd: list[str]):
        self.cmd = cmd

    def __call__(self):
        return run(self.cmd, capture_output=True)
    
def test_class(client):
    cmd = ["echo", "hello", "world"]
    cmd = Command(cmd)
    future = client.submit(cmd)
    print(future.result().stdout.decode())

class Process:
    def __init__(
            self,
            number: int,
            input_id: str, 
            output_id: str,
            client: Client, 
        ) -> None:
        self.number: int = number
        self.input_id: str = input_id
        self.output_id: str = output_id


    
    def execute(self, with_dask: bool, runtime_vars: dict[str, Any]):
        def add(number, input_id, runtime_vars, output_id):
            runtime_vars[output_id] = number + runtime_vars[input_id]

        if with_dask:
            ...
            # future = self.client.submit(
            #     add, 
            #     self.number, 
            #     self.input_id, 
            #     runtime_vars, 
            #     self.output_id
            # )
            # future.result()
        else:
            add( 
                self.number, 
                self.input_id, 
                runtime_vars, 
                self.output_id
            )
        return runtime_vars[self.output_id]


    def __call__(self, with_dask: bool, runtime_vars: dict[str, Any]):
        return self.execute(with_dask, runtime_vars)


class Node:
    def __init__(self, client: Client, processes: list[Process]):
        self.processes: list[Process] = processes
        self.client: Client = client


    def execute(self, with_dask: bool, runtime_vars: dict[str, Any]):
        def wrapper(processes: list[Process], with_dask: bool, runtime_vars: dict[str, Any]):
            """ NOTE
            Dask sends a copy of runtime_vars to a cluster node. This means
            that we have to keep track of new variables that are added to
            runtime_vars. New variables are described with output schemas, but
            for this mock up we simply look at the difference between the old
            and new dictionary.
            """
            old_runtime_vars = deepcopy(runtime_vars)
            for process in processes:
                process.execute(with_dask, runtime_vars)
            return {key: value for key, value in runtime_vars.items() if key not in old_runtime_vars}             

        if with_dask:
            future = self.client.submit(wrapper, deepcopy(self.processes), False, runtime_vars)
            new_runtime_vars = future.result()
            runtime_vars.update(new_runtime_vars)
        else:
            wrapper(self.processes, False, runtime_vars)
            
        
    def __call__(self, with_dask: bool, runtime_vars: dict[str, Any]):
        self.execute(with_dask, runtime_vars)



def main(with_dask):
    client = Client()
    runtime_vars = {"0": 1}
    proc0 = Process(1, "0", "1", client)
    proc1 = Process(2, "1", "2", client)
    proc2 = Process(3, "2", "3", client)
    proc3 = Process(4, "3", "4", client)
    proc4 = Process(5, "4", "5", client)
    proc5 = Process(6, "5", "6", client)
    processes1 = [proc0, proc1, proc2]
    processes2 = [proc3, proc4, proc5]
    node1 = Node(client, processes1)
    node2 = Node(client, processes2)
    node1.execute(with_dask, runtime_vars)
    node2.execute(with_dask, runtime_vars)
    for key in runtime_vars:
        print(key, ":", runtime_vars[key])


if __name__ == "__main__":
    # with_dask = False
    with_dask = True
    main(with_dask)
    