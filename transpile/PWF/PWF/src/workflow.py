import importlib
import inspect
import sys

from abc import abstractmethod
from concurrent.futures import Future
from copy import deepcopy
from dask.distributed import Client, Future
from pathlib import Path
from typing import Any, Optional, Tuple, Union

from .commandlinetool import BaseCommandLineTool
from .process import BaseProcess, Graph, Node
from .utils import Absent, FileObject

class BaseWorkflow(BaseProcess):
    def __init__(
            self,
            main: bool = False,
            # client: Optional[Client] = None,
            runtime_context: Optional[dict] = None,
            loading_context: Optional[dict[str, str]] = None,
            parent_process_id: Optional[str] = None,
            step_id: Optional[str] = None
        ):
        """
        TODO: class description 
        NOTE: if main is True, parent_id and step_id are None
        """
        super().__init__(
            main = main,
            # client = client,
            runtime_context = runtime_context,
            loading_context = loading_context,
            parent_process_id = parent_process_id,
            step_id = step_id
        )

        # TODO Decide if this is the way, see set_groups()
        # self.groupings = ...

        # Mapping of a step ID to its child BaseProcess object
        self.step_id_to_process: dict[str, BaseProcess] = {}

        # Must be overridden in set_steps().
        self.steps: dict[str, dict[str, Any]] = {}

        # Digest workflow file
        self.set_metadata()
        self.set_inputs()
        # self._process_inputs()
        self.set_outputs()
        self.set_requirements()
        self.set_steps()
        self._process_steps()

        # Only the main process executes the workflow.
        if main:
            self.create_dependency_graph()
            self.optimize_dependency_graph()
            self.register_input_sources()
            # self.bind_default_inputs()
            # self.create_task_graph()
            self.execute(self.runtime_context)


    @abstractmethod
    def set_steps(self):
        """
        Defines the sub-processes of this workflow. Overwriting this function
        is mandatory for workflows.
        TODO Desc
        """
        # Example:
        # self.steps = {
        #     "download_images": {
        #         "in": {
        #             "url_list": {
        #                 "source": "url_list"
        #             }
        #         },
        #         "out": "output",
        #         "run": "../steps/DownloadImages.py",
        #         "label": "download_images"
        #     },
        #     "imageplotter": {
        #         "in": {
        #             "input_fits": {
        #                 "source": "download_images/output"
        #             },
        #             "output_image": {
        #                 "default": "before_noise_remover.png"
        #             }

        #         },
        #         "out": "output",
        #         "run": "../steps/ImagePlotter.py",
        #         "label": "imageplotter"
        #     }
        # }
        pass


    def _process_steps(self) -> None:
        """
        TODO Are the runtime_context keys correct?
        TODO Desc
        """
        for step_id, step_dict in self.steps.items():
            # for input_id, input_dict in step_dict["in"].items():
            #     if "default" in input_dict:
            #         subprocess_id: str = self.step_id_to_process[step_id]
            #         subprocess: BaseProcess = self.loading_context["processes"][subprocess_id]
            #         self.runtime_context[subprocess.global_id(input_id)] = input_dict["default"]

            # Register step outputs as global sources
            if isinstance(step_dict["out"], list):
                for out_id in step_dict["out"]:
                    self.runtime_context[self.global_id(step_id + "/" + out_id)] = Absent()
            elif isinstance(step_dict["out"], str):
                self.runtime_context[self.global_id(step_id + "/" + step_dict["out"])] = Absent()
            else:
                raise NotImplementedError("Encountered unsupported type", type(step_dict["out"]))
    



    def set_groups(self) -> None:
        """
        Override to declare which steps should be grouped and executed
        together on a machine.
        NOTE: Not sure if this is the way to do this...
        """
        pass

    
    def create_dependency_graph(self) -> None:
        """ 
        TODO Description
        FIXME Not accurate anymore
        Create a Dask task graph with the sub-processes of this workflow. 
        Can be overwritten to alter execution behaviour. 
        """
        processes: dict[str, BaseProcess] = self.loading_context["processes"]
        graph: Graph = self.loading_context["graph"]

        # Recursively load all processes from steps
        print("[WORKFLOW]: Loading process files:")
        for step_id, step_dict in self.steps.items():
            step_process = self._load_process_from_uri(step_dict["run"], step_id)
            processes[step_process.id] = step_process
            self.step_id_to_process[step_id] = step_process
            node = Node(
                id = step_process.id,
                processes = [step_process],
                is_tool_node = False
            )
            graph.add_node(node)

        # Add tool nodes to the dependency graph
        for tool in processes.values():
            if issubclass(type(tool), BaseCommandLineTool):
                graph.connect_node(
                    node_id = tool.id,
                    parents = get_process_parents(tool),
                    # children = 
                )


    def optimize_dependency_graph(self) -> None:
        """
        TODO Description
        """
        pass


    # def create_task_graph(self) -> None:
    #     """
    #     TODO Description
    #     """
    #     graph: Graph = self.loading_context["graph"]
        
    #     # 
    #     id_to_delayed: dict[str, Delayed] = {}
    #     visited: list[str] = []
    #     frontier: list[str] = deepcopy(graph.roots)
    #     while len(frontier) != 0:
    #         node_id = frontier.pop(0)
    #         node = graph.nodes[node_id]

    #         # Check if parents have been visited
    #         good = True
    #         for parent in node.parents:
    #             # If not all parents have been visited, revisit later
    #             if parent not in visited:
    #                 good = False
    #                 frontier.append(node.id)
    #                 break
    #         if not good: 
    #             continue

    #         parents: list[Delayed] = []
    #         for parent_node_id in node.parents:
    #             parents.append(id_to_delayed[parent_node_id])

    #         # Create and register dask.Delayed tool wrapper
    #         # FIXME This works only for nodes with a single process
    #         node.processes[0].create_task_graph(*parents)
    #         id_to_delayed[node_id] = node.processes[0].task_graph_ref
    #         visited.append(node_id)

    #         # Add children to frontier
    #         for child_node_id in node.children:
    #             if child_node_id not in frontier:
    #                 frontier.append(child_node_id)

    #     if len(visited) != graph.size:
    #         raise Exception("Dangling nodes: Not all nodes have been visited")

    #     def knot(*parents):
    #         """ Tie together all leaves to ensure they are executed """
    #         # print("Finished executing task tree")
    #         pass

    #     leaves: list[Delayed] = []
    #     for node_id in graph.leaves:
    #         # FIXME This works only for nodes with a single process
    #         leaves.append(graph.nodes[node_id].processes[0].task_graph_ref)

    #     self.task_graph_ref = dask.delayed(knot)(leaves)
    #     self.task_graph_ref.visualize(filename="graph.svg")


    def register_input_sources(self) -> None:
        """
        TODO Desc
        NOTE: Only called from the main process.

        """
        if not self.is_main:
            raise Exception("Function should only be called from the main process")

        def get_source_from_step_in(
                process: BaseWorkflow, 
                step_id: str,
                in_id: str
            ) -> Tuple[bool, str]:
            step_in_dict = process.steps[step_id]["in"][in_id]
            if "source" in step_in_dict:
                return False, step_in_dict["source"]
            elif "valueFrom" in step_in_dict:
                return False, step_in_dict["valueFrom"]
            elif "default" in step_in_dict:
                return True, step_in_dict["default"]
            raise NotImplementedError()

        processes: dict[str, BaseProcess] = self.loading_context["processes"]

        # Link tool inputs of each tool to their global source
        for process in processes.values():
            if not issubclass(type(process), BaseCommandLineTool):
                continue

            # The main process gets all its input from the input object
            if process.is_main:
                for input_id in self.inputs:
                    process.input_to_source[input_id] = process.global_id(input_id)
                continue

            # Search for the source of each input.
            for input_id in process.inputs:
                # NOTE: Not a recursive algorithm, because Python has a
                # standard recursion limit and workflows can be huge. The
                # recursion limit can be increased, but the user shouldn't be
                # bothered, so iterative it is.
                if process.step_id is None:
                    raise ValueError("process.step_id cannot be None")
                _step_id: str = process.step_id
                
                _process: BaseWorkflow = processes[process.parent_process_id]
                source = input_id

                while True:
                    use_default, source = get_source_from_step_in(_process, _step_id, source)
                    if use_default:
                        # Input has no dynamic source: Use default value
                        process.input_to_source[input_id] = _process.global_id(input_id)
                        self.runtime_context[_process.global_id(input_id)] = source
                        break
                    
                    if "/" in source:   # {global_process_id}:{step_id}/{output_id}
                        # A step output is the input source
                        process.input_to_source[input_id] = _process.global_id(source)
                        break
                    else:               # {global_process_id}:{input_id}
                        # The input of the parent process is the input source
                        if _process.is_main:
                            # Reached the main process: # Input source is the input object
                            process.input_to_source[input_id] = _process.global_id(source)
                            break

                        # Move to the parent process
                        if _process.step_id is None:
                            raise ValueError("_process.step_id cannot be None")
                        _step_id = _process.step_id
                        _process = processes[_process.parent_process_id]

    
    # def bind_default_inputs(self) -> None:
    #     """
    #     TODO Desc
    #     NOTE: should this be moved somewhere else?
    #     NOTE: Probably shouldn't be used at all
    #     """
    #     for process in self.loading_context["processes"].values():
    #         # Look for defaults in process input
    #         for input_id, input_dict in self.inputs.items():
    #             if "default" in input_dict:
    #                 # self.runtime_context[self.gl] = input_dict["default"]
    #                 raise NotImplementedError()
            
    #         # Look for default in step input
    #         for step_id, step_dict in self.steps.items():
    #             for input_id, input_dict in step_dict["in"].items():
    #                 if "default" in input_dict:
    #                     subprocess: BaseProcess = self.step_id_to_process[step_id]
    #                     source = self.input_to_source[subprocess.global_id(input_id)]
    #                     self.runtime_context[source] = input_dict["default"]


    def build_namespace(self) -> dict[str, Any]:
        """
        Build a local namespace that can be used in eval() calls to evaluate
        expressions that access CWL namespaces, like 'inputs'. Apart from 
        workflow inputs, step inputs are also added to the namespace.
        """
        # Add workflow inputs to namespace
        # BUG FIXME workflow doesnt have input_to_source
        namespace: dict[str, Any] = super().build_namespace()
        print("[WORKFLOW]: RUNTIME_CONTEXT", *[f"{k} :::: {v}" for k, v in self.runtime_context.items()], sep="\n\t")

        # Add step inputs to namespace
        for step_id, step_dict in self.steps.items():
            step_inputs = lambda: None  # Create empty object

            for input_id, step_input_dict in step_dict["in"].items():

                # source = self.input_to_source[input_id]
                step_process: BaseProcess = self.step_id_to_process[step_id]
                source: str = step_process.input_to_source[input_id]
                print(f"SOURCE for {step_id}/{input_id}: {source}")
                value = self.runtime_context[source] # May be Absent()
                if isinstance(value, Absent):
                    # Input not yet available, skip for now
                    break

                # Determine input type and create appropriate object in namespace
                input_dict = step_process.inputs[input_id]
                if "file" in input_dict["type"]:
                    # Create built-in file properties used in CWL expressions
                    if "[]" in input_dict["type"]:
                        # Array of files
                        file_objects = [FileObject(p) for p in value]
                        setattr(step_inputs, input_id, file_objects)
                    else:
                        # Single file
                        setattr(step_inputs, input_id, FileObject(value))
                elif "string" in input_dict["type"]:
                    setattr(step_inputs, input_id, value)
                else:
                    raise NotImplementedError(f"Input type {input_dict['type']} not supported")
            setattr(namespace["inputs"], step_id, step_inputs)
        # print("[WORKFLOW]: NAMESPACE", *namespace["inputs"].__dict__.items(), sep="\n\t")
        print("[WORKFLOW]: NAMESPACE")
        for k, v in namespace["inputs"].__dict__.items():
            print("\t", k)
            print(*[f"\t\t{i}: {j}" for i, j in v.__dict__.items()], sep="\n")
        return namespace
    
 
    def execute_workflow_node(
            self,
            workflow_node: Node,
            runtime_context: dict[str, Any],
            cwl_namespace: dict[str, Any],
            verbose: Optional[bool] = True
        ) -> dict[str, Any]:
            """
            Execute the tasks of this node. 
            
            FIXME: Is the following note still accurate?
            NOTE: Nodes contained in node.graph contain a single 
            BaseCommandLineTool and are executed according to the nodes
            internal dependency graph.
            """
            graph = workflow_node.graph
            if graph is None:
                raise Exception("Node has no graph")

            tool_queue: list[Node] = graph.get_nodes(graph.roots)
            finished: list[Node] = []
            new_runtime_context: dict[str, Any] = {}
            
            # Cycle through tool nodes until all tools have been executed
            while len(tool_queue) != 0 and len(finished) != len(tool_queue):
                # Retrieve tool node from the queue
                tool_node = tool_queue.pop(0)
                
                # Check if the node is ready for execution by checking if all
                # of its parents have finished.
                good: bool = True
                for parent in graph.get_nodes(tool_node.parents):
                    # If not all parents have been visited, re-queue node
                    if parent not in finished:
                        good = False
                        tool_queue.append(tool_node)
                        break
                if not good:
                    continue

                # Dask sends a copy of runtime_context to the cluster node. 
                # This means that outputs have to be registered in the main 
                # runtime_context.
                # Nodes at this level contain a single BaseCommandLineTool.
                process = tool_node.processes[0]
                if not isinstance(process, BaseCommandLineTool):
                    raise TypeError(f"Process {process} is not a BaseCommandLineTool")
                tool: BaseCommandLineTool = process
                print(f"[NODE]: Executing tool {tool.id}")
                result = tool.execute(False, verbose)  # BUG tool.runtime_context not updated with parent runtime context
                runtime_context.update(result)
                new_runtime_context.update(result)
                finished.append(tool_node)
            
            return new_runtime_context
    

    def execute(
            self, 
            runtime_context: dict[str, Any],
            verbose: Optional[bool] = True
        ) -> dict[str, Any]:
        """
        TODO
        """
        # TODO turn on verbose boolean
        verbose = False

        # Update runtime context and build namespace
        self.runtime_context = runtime_context
        cwl_namespace = self.build_namespace()

        # Initialize queues
        graph: Graph = self.loading_context["graph"]
        nodes = graph.nodes
        runnable_nodes: list[Node] = deepcopy(graph.get_nodes(graph.roots))
        runnable: dict[str, Node] = {node.id: node for node in runnable_nodes}
        waiting: dict[str, Node] = {id: node for id, node in nodes.items() if id not in runnable}
        running: dict[str, Tuple[Future, Node]] = {}
        completed: dict[str, Node] = {}
        output: dict[str, Any] = {}

        def lprint():
            s = ", "
            print("[WORKFLOW]: QUEUES")
            print(f"\twaiting: [{s.join([str(graph.short_id[node_id]) for node_id in waiting.keys()])}]")
            # print()
            print(f"\trunnable: [{s.join([str(graph.short_id[node_id]) for node_id in runnable.keys()])}]")
            # print()
            print(f"\trunning: [{s.join([str(graph.short_id[node_id]) for node_id in running.keys()])}]")
            # print()
            print(f"\tcompleted: [{s.join([str(graph.short_id[node_id]) for node_id in completed.keys()])}]")
            print()

        graph.print()
        print("\n".join([f"{short}: {id}" for id, short in graph.short_id.items()]))
        lprint()

        # Polling loop that runs until all graph nodes have been executed.
        # Each node execution is submitted to the Dask scheduler as an async
        # task. The polling loop checks for finished nodes and submits newly
        # executable nodes. Invalid workflows, faulty tools or bad input might
        # result in deadlocks, in which case an exception is raised.
        print("[WORKFLOW]: Executing workflow")
        while len(runnable) != 0 or len(running) != 0:
            # Check for deadlock
            if len(runnable) == 0 and len(running) == 0 and len(waiting) != 0:
                s = "\n\t".join([node_id for node_id in waiting.keys()])
                raise Exception(f"Deadlock detected. Waiting nodes:\n\t{s}")

            # Execute runnable nodes
            for node_id, node in runnable.copy().items():
                client = Client()
                print("[WORKFLOW]: Submitting node", graph.short_id[node_id])
                future = client.submit(
                    self.execute_workflow_node, 
                    node, 
                    self.runtime_context.copy(),
                    cwl_namespace,
                    verbose
                )
                running[node_id] = (future, node)
                runnable.pop(node_id)

            # Check for completed nodes and move runnable children to the
            # running queue.
            for node_id, running_node in running.copy().items(): # running_node: (Future, Node)
                # Remove node from the running list if it has finished
                if running_node[0].done():
                    # Add new runtime state from finished node
                    result = running_node[0].result()
                    print("[WORKFLOW]: RESULTs:\n", *[f"\t{k}: {v}" for k, v in result.items()], sep="\n")
                    output.update(result)
                    self.runtime_context.update(result)

                    # Move node to finished
                    completed[node_id] = running_node[1]
                    running.pop(node_id)
                    lprint()

                    # Add new runnable nodes to queue by checking for each
                    # child if all its parents have completed.
                    for child_id in running_node[1].children:

                        # Only check children that have not already been queued
                        if child_id not in waiting:
                            continue

                        # print("[CHILD]: ", child_id)

                        ready = True
                        for childs_parent_id in nodes[child_id].parents:
                            # print(f"[PARENT]: {childs_parent_id}")      # Signals BUG with duplicate parent entries
                            if childs_parent_id not in completed:
                                print("[WORKFLOW] Parent", {graph.short_id[childs_parent_id]} , "not completed")
                                ready = False
                                break
                        if ready:
                            # All parents have finished: Queue up the child
                            runnable[child_id] = waiting.pop(child_id)
                    print("[WORKFLOW]: Completed node", graph.short_id[running_node[1].id])
                    lprint()
            # time.sleep(0.1)

        # TODO Return new runtime state
        # if "stderr" in output:
            # print(output["stderr"], file=sys.stderr)
        # if "stdout" in output:
            # print(output["stdout"], file=sys.stdout)
        return output


    def _load_process_from_uri(
            self, 
            uri: str,
            step_id: str
        ) -> BaseProcess:
        """
        Dynamic Process loading from file. Raises an exception if no valid 
        BaseProcess sub-class can be found in the file.
        NOTE: Loading a class from a file that contains multiple subclasses of
        BaseProcess causes undefined behaviour.

        Returns:
            Instantiated subclass of BaseProcess. 
        """
        # Taken from:
        # https://stackoverflow.com/questions/66833453/loading-a-class-of-unknown-name-in-a-dynamic-location
        path = Path(uri)
        if not path.is_file():
            raise FileNotFoundError(f"{uri} is not a file")
        
        # Add path of file to PYTHONPATH, so Python can import from it.
        sys.path.append(str(path.parent))
        potential_module = importlib.import_module(path.stem)

        # Test all attributes in the file for being a class
        for potential_class in dir(potential_module):
            obj = getattr(potential_module, potential_class)
            # Check if mysterious class is indeed a class
            if not inspect.isclass(obj):
                continue
            # Check if the class is imported from the actual file uri, and not
            # from an import statement present in the file.
            if not path.stem in str(obj):
                continue
            # Check if the class inherits from BaseProcess, but is not of type
            # BaseProcess. This check is not necessarily needed, but better 
            # be save then sorry.
            if not issubclass(obj, BaseProcess) and not isinstance(obj, BaseProcess):
                continue

            # Instantiate and return the class
            # NOTE: more checks needed?
            print(f"\tFound process at {uri}")
            return obj(
                main = False,
                # client = self.client,
                runtime_context = self.runtime_context,
                loading_context = self.loading_context,
                parent_process_id = self.id,
                step_id = step_id
            )
        raise Exception(f"{uri} does not contain a BaseProcess subclass")
    


    

def get_process_parents(tool: BaseCommandLineTool) -> list[str]:
    """
    TODO Description
    NOTE: How to implement optional args? Handle at runtime!
    """
    # NOTE: Not a recursive algorithm, because Python has a standard recursion 
    # limit and workflows can be huge. The recursion limit can be increased, 
    # but the user shouldn't be bothered, so iterative it is.
    if tool.is_main:
        return []
    
    def get_source(
            step_dict,
            input_id    
        )-> Tuple[bool, Optional[str]]:
        """
        NOTE: How to implement optional args?        
        """
        if "source" in step_dict["in"][input_id]:
            return False, step_dict["in"][input_id]["source"]
        # elif "default" in step_dict["in"][input_id]:
        #     return True, None
        elif "valueFrom" in step_dict["in"][input_id]:
            return True, None
            # return True, step_dict["in"][input_id]["valueFrom"]
            # return True, tool.eval(step_dict["in"][input_id]["valueFrom"])
        elif "default" in step_dict["in"][input_id]:
            return True, None
        raise NotImplementedError(step_dict["in"][input_id])
    
    parents = []
    processes: dict[str, BaseProcess] = tool.loading_context["processes"]

    for input_id in tool.inputs:
        # Start in the parent of tool
        # NOTE Make sure this still works when not working with BaseWorkflow
        process: BaseWorkflow = processes[tool.parent_process_id] 
        step_id = tool.step_id
        step_dict = process.steps[step_id]

        # Go up the process tree, until a tool or the input object
        # is encountered
        while True:
            cont, source = get_source(step_dict, input_id)
            if cont:
                break

            if "/" in source:
                # A step of this process is the input source
                parent_step_id, _ = source.split("/")
                process = process.step_id_to_process[parent_step_id]
                parents.append(process.id)
                break
            else:
                # Parent of this process is the input source
                if process.is_main:
                    # Input comes from input object
                    break
                else:
                    # Input comes from another source upstream
                    process = processes[process.parent_process_id]
                    step_id = process.step_id
                    step_dict = process.steps[step_id]
    return parents