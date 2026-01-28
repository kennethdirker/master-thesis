import concurrent.futures
import dask.distributed
import importlib
import inspect
import sys
import time

from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor   
from copy import deepcopy
from dask.distributed import Client
from pathlib import Path
from types import NoneType
from typing import Any, cast, List, Mapping, Optional, Tuple, TypeAlias, Union

from .commandlinetool import BaseCommandLineTool
from .process import BaseProcess, Graph, Node
from .utils import Absent, FileObject, DirectoryObject, pretty_print_dict, Value, CWL_PY_T_MAPPING, PY_CWL_T_MAPPING

Future: TypeAlias = concurrent.futures.Future | dask.distributed.Future


class BaseWorkflow(BaseProcess):
    # Step info
    step_id_to_process: dict[str, BaseProcess]
    steps: dict[str, dict[str, Any]]

    def __init__(
            self,
            main: bool = True,
            runtime_context: Optional[dict] = None,
            loading_context: Optional[dict[str, str]] = None,
            parent_process_id: Optional[str] = None,
            step_id: Optional[str] = None,
            requirements: Optional[dict[str, Any]] = None
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
            step_id = step_id,
            requirements = requirements
        )

        # TODO Decide if this is the way, see set_groups()
        # self.groupings = ...

        # Mapping of a step ID to its child BaseProcess object
        self.step_id_to_process = {}

        # Must be overridden in set_steps().
        self.steps = {}

        # Digest workflow file
        # self.set_metadata()
        # self.set_inputs()
        # self.set_outputs()
        # self.set_requirements()
        self.set_steps()

        # Only the main process executes the workflow.
        if main:
            self.create_dependency_graph()
            self.register_step_sources()
            self.optimize_dependency_graph()
            self.register_input_sources()
            outputs = self.execute(self.loading_context["use_dask"])
            self.finalize(outputs)


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
                tool = cast(BaseCommandLineTool, tool)
                graph.connect_node(
                    node_id = tool.id,
                    parents = get_process_parents(tool),
                )


    def register_step_sources(self) -> None:
        """
                TODO Desc
        """
        for process in self.loading_context["processes"].values():
            if not issubclass(type(process), BaseWorkflow):
                continue

            for step_id, step_dict in self.steps.items():
                step_proc: BaseProcess = self.step_id_to_process[step_id]
                
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
            ) -> Tuple[bool, str | None]:
            """
            Returns:
                (is_static, source)
                is_static (bool):
                    True if the input source is not dynamic source and the
                    default value should be used.
                source (str):
                    The source of the input. If is_static is True, this is the
                    default value of the input. If is_static is False, this is
                    the source ID of the input.
            """
            step_in_dict = process.steps[step_id]["in"][in_id]
            if "source" in step_in_dict:
                return False, step_in_dict["source"]
            elif "default" in step_in_dict:
                return True, step_in_dict["default"]
            elif "valueFrom" in step_in_dict:
                return True, None
                # return True, step_in_dict["valueFrom"]
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

                _process = cast(BaseWorkflow, processes[cast(str, process.parent_process_id)])
                source = input_id

                while True:
                    is_static, source = get_source_from_step_in(cast(BaseWorkflow, _process), _step_id, source)
                    if is_static:
                        # Input comes from default or valueFrom
                        process.input_to_source[input_id] = _process.global_id(_step_id + "/" + input_id)
                        if source is None:
                            value = Absent("Value comes from valueFrom and is not yet filled in")
                        else:
                            value = Value(source, type(source), PY_CWL_T_MAPPING[type(source)][0])

                        self.runtime_context[process.input_to_source[input_id]] = value
                        break

                    source = cast(str, source)
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
                        
                        _process = processes[cast(str, _process.parent_process_id)]

    
    def build_step_namespace(
            self, 
            tool:  BaseCommandLineTool,
            runtime_context: dict[str, Any],
        ) -> dict[str, Any]:
        """
        """
        if tool.parent_process_id is None or tool.step_id is None:
            raise ValueError("Tool must have parent_process_id and step_id defined")
        # Add workflow inputs to namespace
        namespace = tool.build_namespace()

        # Add step inputs to namespace
        if tool.step_id is None:
            raise ValueError("tool.step_id cannot be None")
        
        workflow = self.loading_context["processes"][tool.parent_process_id]
        workflow = cast(BaseWorkflow, workflow)
        for in_id, in_dict in workflow.steps[tool.step_id]["in"].items():
            input_dict = tool.inputs[in_id]
            source = tool.input_to_source[in_id]
            v = runtime_context[source]
            if isinstance(v, (Absent, NoneType)):
                namespace["inputs"][in_id] = None
                continue
            if not isinstance(v, Value):
                raise Exception(f"Runtime_context item is not wrapped in Value: Found '{type(v)}'")

            # Add the value to the namespace. If the value is a string,
            # we have to check if it needs to be converted to a FileObject
            # or a DirectoryObject.
            if isinstance(v.value, str):
                if v.is_array:
                    if "file[]" in input_dict["type"]:
                        file_objects = [FileObject(p) for p in v.value]
                        namespace["inputs"][in_id] = file_objects
                    if "directory[]" in input_dict["type"]:
                        dir_objects = [DirectoryObject(p) for p in v.value]
                        namespace["inputs"][in_id] = dir_objects
                    else:
                        namespace["inputs"][in_id] = v.value
                else:
                    if "file" in input_dict["type"]:
                        namespace["inputs"][in_id] = FileObject(v.value)
                    if "directory" in input_dict["type"]:
                        namespace["inputs"][in_id] = DirectoryObject(v.value)
                    else:
                        namespace["inputs"][in_id] = v.value
            elif v.type in PY_CWL_T_MAPPING:
                namespace["inputs"][in_id] = v.value
            else:
                raise Exception(f"Found unsupported type '{v.type}")
                
        return namespace
    

    def prepare_step_runtime_context(
            self,
            tool: BaseCommandLineTool, 
            runtime_context: dict[str, Value]
        ) -> dict[str, Value]:
        """
        Create a new runtime context dictionary with updated source values
        from process input and step input valueFrom fields.
        """
        if tool.parent_process_id is None or tool.step_id is None:
            raise ValueError("Tool must have parent_process_id and step_id defined")
        parent_workflow_id = tool.parent_process_id
        parent_process = cast(BaseWorkflow, self.loading_context["processes"][parent_workflow_id])

        # Create a copy to prevent replacing source values with valueFrom values
        step_runtime_context: dict[str, Value] = runtime_context.copy()

        # Create cwl_namespace with step ins and runtime_namespace
        cwl_namespace = self.build_step_namespace(tool, runtime_context)

        # for each input check:
        for input_id, input_dict in tool.inputs.items():
            if not "valueFrom" in parent_process.steps[tool.step_id]["in"][input_id]:
                continue

            # Evaluate valueFrom. Before evaluation, 'self' needs to be set in
            # the namespace.
            source = tool.input_to_source[input_id]
            value = step_runtime_context[source]
            if value:
                value = value.value
            cwl_namespace["self"] = value
            expression = parent_process.steps[tool.step_id]["in"][input_id]["valueFrom"]
            expr_result = self.eval(expression, cwl_namespace)
            # Evaluated expression needs to be wrapped
            # NOTE TO SELF: If string overlap with files and dirs is handled in commandlinetool, skip it here?
            # BUG js2py returns lists and dicts as js2py.base.JsObjectWrapper.
            # How to handle this?
            if isinstance(expr_result, List):
                if len(expr_result) == 0:
                    # TODO List items can be any type, what to do?
                    ...
                if not type(expr_result[0]) in PY_CWL_T_MAPPING:
                    raise Exception(f"Found unsupported item type in array: '{type(expr_result[0])}'")
                
                value = Value(expr_result, type(expr_result[0]), PY_CWL_T_MAPPING[type(expr_result[0])][0])
            elif isinstance(expr_result, Mapping):
                raise NotImplementedError("Maps are not supported as a type")
            else:
                if not type(expr_result) in PY_CWL_T_MAPPING:
                    raise Exception(f"Found unsupported type: '{type(expr_result)}'")
                value = Value(expr_result, type(expr_result), PY_CWL_T_MAPPING[type(expr_result)][0])
            step_runtime_context[source] = value
        return step_runtime_context


 
    def execute_workflow_node(
            self,
            workflow_node: Node,
            runtime_context: dict[str, Any],
            verbose: Optional[bool] = True,
            executor: Optional[ThreadPoolExecutor] = None
        ) -> dict[str, Any]:
        """
        Execute the tasks of this node. 
        TODO Only tested on nodes containing a single tool node!
        """
        graph = workflow_node.graph
        if graph is None:
            raise Exception("Node has no graph")

        if executor is None:
            executor = ThreadPoolExecutor()

        nodes = graph.nodes
        runnable_nodes: list[Node] = deepcopy(graph.get_nodes(graph.roots))
        runnable: dict[str, Node] = {node.id: node for node in runnable_nodes}
        waiting: dict[str, Node] = {id: node for id, node in nodes.items() if id not in runnable}
        running: dict[str, Tuple[Future, Node]] = {}
        completed: dict[str, Node] = {}
        outputs: dict[str, Value] = {}
        while len(runnable) != 0 or len(running) != 0:
            # Execute runnable nodes in the ThreadPool
            for node_id, node in runnable.copy().items():
                # These nodes (which are wrapped tools) contain a single 
                # node in their graph.
                tool = cast(BaseCommandLineTool, node.processes[0])
                step_runtime_context = self.prepare_step_runtime_context(tool, runtime_context)
                print(f"[NODE]: Executing tool {tool.id}")
                future = executor.submit(
                    tool.execute,
                    False, 
                    step_runtime_context, 
                    verbose
                )
                running[node_id] = (future, node)
                runnable.pop(node_id)
            
            # Check for completed tools and move runnable children to the
            # running queue.
            for node_id, running_task in running.copy().items():
                if running_task[0].done():
                    # Save results from finished tool and remove tool from
                    # the running list. Runtime_context is updated for the
                    # next tool.
                    result: dict[str, Value] = running_task[0].result()
                    outputs.update(result)
                    runtime_context.update(result)

                    # Move tool to finished
                    completed[node_id] = running_task[1]
                    running.pop(node_id)

                    # Add new runnable tools to queue by checking for each
                    # child if all its parents have completed.
                    for child_id in running_task[1].children:
                        # Only check waiting children
                        if child_id not in waiting:
                            continue

                        ready = True
                        for childs_parent_id in nodes[child_id].parents:
                            if childs_parent_id not in completed:
                                ready = False
                                break
                        if ready:
                            runnable[child_id] = waiting.pop(child_id)
            time.sleep(0.1)

        # Check for deadlock
        if len(runnable) == 0 and len(running) == 0 and len(waiting) != 0:
            s = "\n\t".join([node_id for node_id in waiting.keys()])
            raise Exception(f"Deadlock detected. Waiting nodes:\n\t{s}")

        return outputs
    

    def execute(
            self, 
            use_dask: bool,
            verbose: bool = True,
            dask_client: Optional[Client] = None
        ) -> dict[str, Value]:
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
        if use_dask and dask_client is None:
            client = Client()
        else:
            client = ThreadPoolExecutor()

        # Initialize queues
        graph: Graph = self.loading_context["graph"]
        nodes = graph.nodes
        runnable_nodes: list[Node] = deepcopy(graph.get_nodes(graph.roots))
        runnable: dict[str, Node] = {node.id: node for node in runnable_nodes}
        waiting: dict[str, Node] = {id: node for id, node in nodes.items() if id not in runnable}
        running: dict[str, Tuple[Future, Node]] = {}
        completed: dict[str, Node] = {}
        outputs: dict[str, Value] = {}

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
            # Execute runnable nodes
            for node_id, node in runnable.copy().items():
                if verbose: print("[WORKFLOW]: Submitting node", graph.short_id[node_id])

                # If dask is disabled, the ThreadPoolExecutor client is used to
                # get concurrently executed tasks. If Dask is used, a 
                # ThreadPoolExecutor client is created on the execution node.
                future = client.submit(
                    self.execute_workflow_node, 
                    node, 
                    self.runtime_context.copy(),
                    verbose,
                    client if isinstance(client, ThreadPoolExecutor) else None
                )

                running[node_id] = (future, node)
                runnable.pop(node_id)

            # Check for completed nodes and move runnable children to the
            # running queue.
            for node_id, running_task in running.copy().items(): # running_task: (Future, Node)
                # Remove node from the running list if it has finished
                if running_task[0].done():
                    # Add new runtime state from finished node
                    result: dict[str, Value] = running_task[0].result()
                    outputs.update(result)
                    self.runtime_context.update(result)

                    # Move node to finished
                    completed[node_id] = running_task[1]
                    running.pop(node_id)
                    lprint()

                    # Add new runnable nodes to queue by checking for each
                    # child if all its parents have completed.
                    for child_id in running_task[1].children:

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
                        print("[WORKFLOW]: Completed node", graph.short_id[running_task[1].id])
                        lprint()
            time.sleep(0.1)

        # Check for deadlock
        if len(runnable) == 0 and len(running) == 0 and len(waiting) != 0:
            s = "\n\t".join([node_id for node_id in waiting.keys()])
            raise Exception(f"Deadlock detected. Waiting nodes:\n\t{s}")
        
        # Output keys are prefixed with the global ID of the tool that created
        # the outputs, resulting in a mismatch between that key  
        new_outputs: dict[str, Any] = {}
        for output_id, output_dict in self.outputs.items():
            step_id, step_output_id = output_dict["outputSource"].split("/")
            value = outputs[self.step_id_to_process[step_id].global_id(step_output_id)]
            new_outputs[self.global_id(output_id)] = value

        return new_outputs


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
        process = cast(BaseWorkflow, processes[cast(str, tool.parent_process_id)])
        step_id = tool.step_id
        step_dict = process.steps[cast(str, step_id)]

        # Go up the process tree, until a tool or the input object
        # is encountered
        while True:
            cont, source = get_source(step_dict, input_id)
            if cont:
                break

            source = cast("str", source)
            if "/" in source:
                # A step of this process is the input source
                parent_step_id, _ = source.split("/")
                process = cast(BaseWorkflow, process).step_id_to_process[parent_step_id]
                parents.append(process.id)
                break
            else:
                # Parent of this process is the input source
                if process.is_main:
                    # Input comes from input object
                    break
                else:
                    # Input comes from another source upstream
                    process = processes[cast(str, process.parent_process_id)]
                    step_id = process.step_id
                    step_dict = cast(BaseWorkflow, process).steps[cast(str, step_id)]
    return parents