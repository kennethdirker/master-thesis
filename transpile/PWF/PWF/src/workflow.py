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

"""
NOTE TO SELF: WHERE WE LEFT OFF
Default values in workflow steps are not processed correctly.
I think they might be overwritten by valueFrom expressions.

"""

class BaseWorkflow(BaseProcess):
    def __init__(
            self,
            main: bool = True,
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
        self.set_outputs()
        self.set_requirements()
        self.set_steps()

        # Only the main process executes the workflow.
        if main:
            self.create_dependency_graph()
            self._process_steps()       # BUG Now only handles the main workflow file
            self.optimize_dependency_graph()
            self.register_input_sources()
            self.execute()


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
                )


    def _process_steps(self) -> None:
        """
        BUG FIXME Now only handles the main workflow file, should handle all
                TODO Desc
        """
        for process in self.loading_context["processes"].values():
            if not issubclass(type(process), BaseWorkflow):
                continue

            for step_id, step_dict in self.steps.items():
                step_proc: BaseProcess = self.step_id_to_process[step_id]
                
                # Register step inputs with expressions and default values as global sources
                # TODO FIXME Uncomment if needed
                # for input_id, input_dict in step_dict["in"].items():
                #     if "default" in input_dict:
                #         self.runtime_context[step_proc.global_id(input_id)] = input_dict["default"]
                #     if "valueFrom" in input_dict:
                #         # tool = self.step_id_to_process[step_id]
                #         self.runtime_context[process.global_id(input_id)] = input_dict["valueFrom"]

                # Register step outputs as global sources
                if isinstance(step_dict["out"], list):
                    for out_id in step_dict["out"]:
                        self.runtime_context[step_proc.global_id(out_id)] = Absent()
                elif isinstance(step_dict["out"], str):
                    self.runtime_context[step_proc.global_id(step_dict["out"])] = Absent()
                else:
                    raise NotImplementedError("Encountered unsupported type", type(step_dict["out"]))
    


    def optimize_dependency_graph(self) -> None:
        """
        TODO Description
        """
        pass


    def register_input_sources(self) -> None:
        """
        Register the global source of each tool input in the workflow. The
        source is stored in the tool's input_to_source dictionary. The source
        is a key in the runtime_context dictionary that contains the actual
        value of the input. 
        NOTE: Only called from the main process.

        """
        if not self.is_main:
            raise Exception("Function should only be called from the main process")

        def get_source_from_step_in(
                process: BaseWorkflow, 
                step_id: str,
                in_id: str
            ) -> Tuple[bool, str]:
            """
            Returns:
                (is_static, source)
                is_static: bool
                    True if the input has no dynamic source and the default value
                    should be used.
                source: str
                    The source of the input. If is_static is True, this is the
                    default value of the input. If is_static is False, this is
                    the source of the input.
            """
            step_in_dict = process.steps[step_id]["in"][in_id]
            if "source" in step_in_dict:
                return False, step_in_dict["source"]
            elif "valueFrom" in step_in_dict:
                return True, step_in_dict["valueFrom"]
                # return False, step_in_dict["valueFrom"]
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
                    is_static, source = get_source_from_step_in(_process, _step_id, source)
                    if is_static:
                        # Input has no dynamic source: Use default value
                        process.input_to_source[input_id] = _process.global_id(_step_id + "/" + input_id)
                        print(f"[ASSIGN] {source} -> {process.input_to_source[input_id]}")
                        self.runtime_context[process.input_to_source[input_id]] = source
                        break
                    
                    if "/" in source:   # {global_process_id}:{step_id}/{output_id}
                        # A step output is the input source
                        step_id, output_id = source.split("/")
                        output_process = self.step_id_to_process[step_id]
                        process.input_to_source[input_id] = output_process.global_id(output_id)
                        break
                    else:               # {global_process_id}:{input_id}
                        # The input of the parent process is the input source
                        if _process.is_main:
                            # Reached the main process: Input source is the input object
                            process.input_to_source[input_id] = _process.global_id(source)
                            break

                        # Move to the parent process
                        if _process.step_id is None:
                            raise ValueError("_process.step_id cannot be None")
                        _step_id = _process.step_id
                        _process = processes[_process.parent_process_id]

    
    def build_step_namespace(
            self, 
            tool:  BaseCommandLineTool,
            runtime_context: dict[str, Any],
        ) -> dict[str, Any]:
        """
        """
        # Add workflow inputs to namespace

        # TODO FIXME BEGIN Gather process inputs
        # namespace = process.build_namespace()
        namespace = {}
        inputs = lambda: None   # Create empty object
        namespace["inputs"] = inputs
        # TODO END

        inputs: object = namespace["inputs"]

        # Add step inputs to namespace
        # for input_id, input_step_dict in self.steps[tool.step_id]["in"].items():
        for input_id in self.steps[tool.step_id]["in"]:
            source = tool.input_to_source[input_id]
            value = runtime_context[source]

            input_dict = tool.inputs[input_id]
            if "file" in input_dict["type"]:
                # Create built-in file properties used in CWL expressions
                if "[]" in input_dict["type"]:
                    # Array of files
                    file_objects = [FileObject(p) for p in value]
                    setattr(inputs, input_id, file_objects)
                else:
                    # Single file
                    setattr(inputs, input_id, FileObject(value[0]))
            elif "string" in input_dict["type"]:
                setattr(inputs, input_id, value)
            else:
                raise NotImplementedError(f"Input type {input_dict['type']} not supported")
        namespace["inputs"] = inputs

        return namespace
    

    def update_sources(
            self,
            tool: BaseCommandLineTool, 
            runtime_context: dict[str, Any]
        ) -> None:
        """
        Update the sources of the tool's inputs by evaluating any valueFrom
        expressions. The evaluated value is stored in the runtime_context
        dictionary with the source as key.
        """
        if tool.parent_process_id is None or tool.step_id is None:
            raise ValueError("Tool must have parent_process_id and step_id defined")
        # parent_process: BaseWorkflow = self.loading_context["processes"][tool.parent_process_id]
        
        for input_id in tool.inputs:
            source = tool.input_to_source[input_id]
            source_split = source.split(":")
            source_tail = source_split[-1]          # Get input ID or expression

            if source_tail.startswith("$") and source.endswith("$"):
                # Input is a valueFrom expression
                process_id = ":".join(source_split[0:2]) # Get parent workflow ID
                process: BaseWorkflow = self.loading_context["processes"][process_id]
                
                # Build CWL namespace for expression evaluation and evaluate
                cwl_namespace = process.build_step_namespace(tool, runtime_context)
                expression = source_tail[1:-1]  # Removes the $ symbols
                value = self.eval(expression, cwl_namespace)
                runtime_context[source] = value


 
    def execute_workflow_node(
            self,
            workflow_node: Node,
            runtime_context: dict[str, Any],
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
                # cwl_namespace = self.build_namespace()
                self.update_sources(tool, runtime_context)
                result = tool.execute(False, runtime_context, verbose)  # BUG tool.runtime_context not updated with parent runtime context
                # result = tool.execute(False, cwl_namespace, verbose)  # BUG tool.runtime_context not updated with parent runtime context
                runtime_context.update(result)
                new_runtime_context.update(result)
                finished.append(tool_node)
            
            return new_runtime_context
    

    def execute(
            self, 
            verbose: Optional[bool] = True,
            client: Optional[Client] = None
        ) -> dict[str, Any]:
        """
        Execute the workflow as the main process. The workflow is executed by
        submitting each node of the workflow dependency graph to the Dask
        scheduler as an asynchronous task. The workflow is executed in a polling
        loop that checks for finished nodes and submits newly executable nodes.
        The function returns when all nodes have been executed.
        NOTE: This function should only be called from the main process.
        Returns:
            Dictionary of (output ID, output value) key-value pairs.
        """
        if client is None:
            client = Client()

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
            print(f"\trunnable: [{s.join([str(graph.short_id[node_id]) for node_id in runnable.keys()])}]")
            print(f"\trunning: [{s.join([str(graph.short_id[node_id]) for node_id in running.keys()])}]")
            print(f"\tcompleted: [{s.join([str(graph.short_id[node_id]) for node_id in completed.keys()])}]")
            print()

        if verbose:
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
                if verbose: print("[WORKFLOW]: Submitting node", graph.short_id[node_id])
                future = client.submit(
                    self.execute_workflow_node, 
                    node, 
                    self.runtime_context.copy(),
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

                        ready = True
                        for childs_parent_id in nodes[child_id].parents:
                            if childs_parent_id not in completed:
                                print("[WORKFLOW] Parent", {graph.short_id[childs_parent_id]} , "not completed")
                                ready = False
                                break
                        if ready:
                            # All parents have finished: Queue up the child
                            runnable[child_id] = waiting.pop(child_id)
                    if verbose:
                        print("[WORKFLOW]: Completed node", graph.short_id[running_node[1].id])
                        lprint()
            # time.sleep(0.1)
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
        TODO: How to implement optional args?        
        """
        if "source" in step_dict["in"][input_id]:
            return False, step_dict["in"][input_id]["source"]
        elif "valueFrom" in step_dict["in"][input_id]:
            return True, None
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