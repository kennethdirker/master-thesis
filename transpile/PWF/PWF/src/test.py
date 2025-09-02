









# # ======================================================================================
# """
# PROBLEM: dask.tokenize.TokenizationError cannot be deterministically hashed
# Cause: The Process class contained a reference to a Dask client, which cannot be serialized by pickle.
# FIX: Create a new client locally in the process, instead of reusing a client. 
# """


# from dask.distributed import Client
# from subprocess import run, CompletedProcess
# from copy import deepcopy
# from typing import Any
# from dask.base import tokenize, normalize_token
# import dask
# dask.config.set({'logging.distributed': 'error', 'logging.distributed': 'error'})


# class Process:
#     def __init__(
#             self,
#             number: int,
#             input_id: str, 
#             output_id: str,
#             # client: Client, 
#         ) -> None:
#         self.number: int = number
#         self.input_id: str = input_id
#         self.output_id: str = output_id
#         # self.client = client

#     def __dask_tokenize__(self):
#         return normalize_token(Process), self.number, self.input_id, self.output_id
#         # return normalize_token(Process), self.number, self.input_id, self.output_id, self.client
    

    
#     def execute(self, with_dask: bool, runtime_vars: dict[str, Any]):
#         def add(number, input_id, runtime_vars, output_id):
#             runtime_vars[output_id] = number + runtime_vars[input_id]
#         if with_dask:
#             client = Client()
#             future = client.submit(
#                 add, 
#                 self.number, 
#                 self.input_id, 
#                 runtime_vars, 
#                 self.output_id
#             )
#             future.result()
#         else:
#             add( 
#                 self.number, 
#                 self.input_id, 
#                 runtime_vars, 
#                 self.output_id
#             )
#         return runtime_vars[self.output_id]


#     def __call__(self, with_dask: bool, runtime_vars: dict[str, Any]):
#         return self.execute(with_dask, runtime_vars)


# class Node:
#     # def __init__(self, client: Client, processes: list[Process]):
#     def __init__(self, processes: list[Process]):
#         self.processes: list[Process] = processes
#         # self.client: Client = client


#     def execute(self, with_dask: bool, runtime_vars: dict[str, Any]):
#         # print("dask 2:", with_dask)
#         def wrapper(processes: list[Process], with_dask: bool, runtime_vars: dict[str, Any]):
#             """ NOTE
#             Dask sends a copy of runtime_vars to a cluster node. This means
#             that we have to keep track of new variables that are added to
#             runtime_vars. New variables are described with output schemas, but
#             for this mock up we simply look at the difference between the old
#             and new dictionary.
#             """
#             # print("dask 3:", with_dask)
#             old_runtime_vars = deepcopy(runtime_vars)
#             for process in processes:
#                 process.execute(with_dask, runtime_vars)
#             return {key: value for key, value in runtime_vars.items() if key not in old_runtime_vars}             

#         if with_dask:
#             process_list = self.processes.copy()
#             client = Client()
#             future = client.submit(wrapper, process_list, False, runtime_vars)
#             # future = self.client.submit(wrapper, process_list, False, runtime_vars)
#             # future = self.client.submit(wrapper, self.processes.copy(), False, runtime_vars)
#             # future = self.client.submit(wrapper, deepcopy(self.processes), False, runtime_vars)
#             new_runtime_vars = future.result()
#             runtime_vars.update(new_runtime_vars)
#         else:
#             wrapper(self.processes, False, runtime_vars)
            
        
#     def __call__(self, with_dask: bool, runtime_vars: dict[str, Any]):
#         self.execute(with_dask, runtime_vars)



# def main(with_dask):
#     runtime_vars = {"0": 1}
#     proc0 = Process(1, "0", "1",)
#     proc1 = Process(2, "1", "2",)
#     proc2 = Process(3, "2", "3",)
#     proc3 = Process(4, "3", "4",)
#     proc4 = Process(5, "4", "5",)
#     proc5 = Process(6, "5", "6",)
#     processes1 = [proc0, proc1, proc2]
#     processes2 = [proc3, proc4, proc5]
#     node1 = Node(processes1)
#     node2 = Node(processes2)
#     # print("dask 1:", with_dask)
#     node1.execute(with_dask, runtime_vars)
#     node2.execute(with_dask, runtime_vars)
#     for key in runtime_vars:
#         print(key, ":", runtime_vars[key])
#     # client = Client()
#     # runtime_vars = {"0": 1}
#     # proc0 = Process(1, "0", "1", client)
#     # proc1 = Process(2, "1", "2", client)
#     # proc2 = Process(3, "2", "3", client)
#     # proc3 = Process(4, "3", "4", client)
#     # proc4 = Process(5, "4", "5", client)
#     # proc5 = Process(6, "5", "6", client)
#     # processes1 = [proc0, proc1, proc2]
#     # processes2 = [proc3, proc4, proc5]
#     # node1 = Node(client, processes1)
#     # node2 = Node(client, processes2)
#     # print("dask 1:", with_dask)
#     # node1.execute(with_dask, runtime_vars)
#     # node2.execute(with_dask, runtime_vars)
#     # for key in runtime_vars:
#     #     print(key, ":", runtime_vars[key])


# if __name__ == "__main__":
#     import time
#     # with_dask = False
#     with_dask = True
#     main(with_dask)
#     print("Going to sleep")
#     time.sleep(5)
#     print("Yawn, I just woke up")
    