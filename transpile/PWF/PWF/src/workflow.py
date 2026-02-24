from __future__ import annotations

import concurrent.futures
import dask.distributed
import importlib
import inspect
import sys
import time

from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor   
from dask.distributed import Client
from pathlib import Path
from types import NoneType
from typing import (
    Any,
    cast,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    TypeAlias,
)
from uuid import uuid4

from .process import BaseProcess
from .commandlinetool import BaseCommandLineTool
from .utils import (
    Absent,
    FileObject,
    DirectoryObject,
    Value,
    CWL_PY_T_MAPPING,
    PY_CWL_T_MAPPING,
)

Future: TypeAlias = concurrent.futures.Future | dask.distributed.Future


def get_process_parents_ids(tool: BaseCommandLineTool) -> List[str]:
    """
    TODO Description
    """
    # NOTE: Iterative algorithm because I initially thought python has a rather
    # low standard recursion limit, which I did not want to touch. However, the
    # default recursion limit is actually a 1000 levels, which is way deeper
    # than any workflow should realisticly get. Oh well. 
    if tool.is_main or tool.parent_workflow_id is None:
        return []
    
    def get_source(
            step_dict,
            input_id    
        )-> Tuple[bool, Optional[str]]:
        """
        TODO Description        
        """
        if "source" in step_dict["in"][input_id]:
            return False, step_dict["in"][input_id]["source"]
        elif "valueFrom" in step_dict["in"][input_id]:
            return True, None
        elif "default" in step_dict["in"][input_id]:
            return True, None
        raise NotImplementedError(step_dict["in"][input_id])

    parents: List[str] = []
    processes: Dict[str, BaseProcess] = tool.loading_context["processes"]

    for input_id in tool.inputs:
        # Start in the parent of tool
        process = cast(BaseWorkflow, processes[cast(str, tool.parent_workflow_id)])
        step_id = tool.step_id
        step_dict = process.steps[cast(str, step_id)]

        # Go up the process tree, until a tool or the input object
        # is encountered
        while True:
            cont, source = get_source(step_dict, input_id)
            if cont:
                break

            source = cast(str, source)
            if "/" in source:
                # A step of this workflow is the input source
                output_step_id, output_id = source.split("/")
                process = cast(BaseWorkflow, process).step_id_to_process[output_step_id]
                
                if issubclass(type(process), BaseCommandLineTool):
                    # Found the parent tool
                    parents.append(process.id)
                    break
                
                # Source comes from another sub-workflow
                output_source = process.outputs[output_id]["outputSource"]
                step_id, input_id = output_source.split("/")
                step_dict = cast(BaseWorkflow, process).steps[step_id]
            else:
                # Parent of this process is the input source
                if process.is_main or process.parent_workflow_id is None:
                    # Input comes from input object and has no source process
                    break
                else:
                    # Input comes from a source in the parent workflow
                    process = cast(BaseWorkflow, processes[process.parent_workflow_id])
                    step_id = cast(str, step_id)
                    step_dict = cast(BaseWorkflow, process).steps[step_id]
    return parents


class BaseWorkflow(BaseProcess):
    # Step info
    step_id_to_process: Dict[str, BaseProcess]
    steps: Dict[str, Dict[str, Any]]
    # groupings

    def __init__(
            self,
            main: bool = True,
            loading_context: Optional[Dict[str, str]] = None,
            parent_process_id: Optional[str] = None,
            step_id: Optional[str] = None,
            inherited_requirements: Optional[Dict[str, Any]] = None
        ):
        """
        TODO: class description 
        # TODO Explain loading_context
            - graph

        NOTE: if main is True, the process has no parents and its 
        parent_id and step_id are None.
        """
        super().__init__(
            main = main,
            loading_context = loading_context,
            parent_process_id = parent_process_id,
            step_id = step_id
        )

        # TODO Decide if this is the way, see set_groups()
        # self.groupings = ...

        # Mapping of a step ID to its child BaseProcess-derived object
        self.step_id_to_process = {}

        # Must be overridden in set_steps().
        self.steps = {}

        # Digest workflow file
        self.set_steps()
        # self.set_groupings()    # <- Not sure if this is the way to do this...
        self.load_step_processes()
        self.process_requirements(inherited_requirements)

        # The top-level (main) BaseWorkflow class builds and executes the
        # the workflow.
        if main:
            yaml_uri = self.loading_context["input_object"]
            runtime_context = self.load_input_object(yaml_uri)
            self.register_step_output_sources(runtime_context)
            self.register_input_sources(runtime_context)
            self.create_dependency_graph()
            self.optimize_dependency_graph()
            outputs = self.execute(self.loading_context["use_dask"],
                                   runtime_context,
                                   dask_client = None)
            self.finalize(outputs)


    @abstractmethod
    def set_steps(self):
        """
        Defines the sub-processes of this workflow. Overwriting this function
        is mandatory for workflows.
        """
        pass


    def process_requirements(
            self,
            inherited_requirements: Dict[str, Any] | None
        ) -> None:
        """
        Set the requirements dict with inhertited requirements and override
        them if present in this tool.
        """
        if inherited_requirements is None:
            return

        updated_reqs = inherited_requirements
        for req_key, req_body in self.requirements.items():
            updated_reqs[req_key] = req_body
        self.requirements = updated_reqs


    def set_groupings(self) -> None:
        """
        Override to declare which steps should be grouped and executed
        together on a machine.
        NOTE: Not sure if this is the way to do this...
        """
        pass
        # self.groups = {}
        # self.groups = []


    def load_process_from_uri(
            self, 
            uri: str,
            step_id: str,
            requirements: Dict[str, Any],
            verbose: bool = False
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
            if verbose: print(f"\tFound process at {uri}")

            return obj(
                main = False,
                loading_context = self.loading_context,
                parent_process_id = self.id,
                step_id = step_id,
                inherited_requirements = requirements
            )
        raise Exception(f"{uri} does not contain a BaseProcess subclass")
    

    def load_step_processes(self) -> None:
        """
        Load all sub-processes into self.loading_context["processes"]. 
        Because the BaseWorkflow constructor calls this function, all
        sub-processes of the main workflow class are loaded recursively.
        """
        for step_id, step_dict in self.steps.items():
            sub_process = self.load_process_from_uri(step_dict["run"], step_id, self.requirements)
            self.loading_context["processes"][sub_process.id] = sub_process
            self.step_id_to_process[step_id] = sub_process


    def register_step_output_sources(
            self,
            runtime_context: Dict[str, Any]
        ) -> None:
        """
        Add an empty entry in the ``runtime_context`` for each step output.
        Empty entries are used to see which tools have not supplied their
        outputs.
        """
        for process in self.loading_context["processes"].values():
            if not issubclass(type(process), BaseWorkflow):
                continue

            for step_id, step_dict in self.steps.items():
                step_proc: BaseProcess = self.step_id_to_process[step_id]
                
                # Register step outputs as global sources
                if isinstance(step_dict["out"], list):
                    for out_id in step_dict["out"]:
                        runtime_context[step_proc.global_id(out_id)] = Absent()
                elif isinstance(step_dict["out"], str):
                    runtime_context[step_proc.global_id(step_dict["out"])] = Absent()
                else:
                    raise NotImplementedError("Encountered unsupported type", type(step_dict["out"]))


    def register_input_sources(
            self,
            runtime_context: Dict[str, Any]
        ) -> None:
        """
        Register each tool input as a global source for all processes in the 
        workflow. Sources are stored in the tool's input_to_source dictionary. 
        In turn, sources are keys in runtime_context dictionaries that contains
        the actual value of the input. 
        NOTE: Should only be called from the main process.
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
            # NOTE get_process_parents_ids does it in this order, should it work like this?
            elif "default" in step_in_dict:
                return True, step_in_dict["default"]
            elif "valueFrom" in step_in_dict:
                return True, None
            # elif "default" in step_in_dict:
            #     return True, step_in_dict["default"]
            # elif "valueFrom" in step_in_dict:
            #     return True, None
            raise NotImplementedError()

        processes: Dict[str, BaseProcess] = self.loading_context["processes"]

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

                _process = cast(BaseWorkflow, processes[cast(str, process.parent_workflow_id)])
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

                        runtime_context[process.input_to_source[input_id]] = value
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
                        
                        _process = processes[cast(str, _process.parent_workflow_id)]


    def create_dependency_graph(self) -> None:
        """ 
        Build the dependency graph for this workflow. The OuterGraph describes
        the relations between independent execution groups. Each execution
        group contains an InnerGraph of tool-containing nodes. Tools withing
        the same InnerGraph are executed on the same compute node.

        Initially, the completed dependency graph contains an execution group
        for every tool. ``optimize_dependency_graph()`` can merge execution
        groups to have data dependent tasks execute within the same locality.
        """
        processes: dict[str, BaseProcess] = self.loading_context["processes"]
        graph = self.loading_context["graph"] = OuterGraph()
        tool_id_to_node: Dict[str, OuterNode] = {}

        # Workflow processes essentially describe the edges between the tool
        # nodes and are not added as executable nodes themselves.

        # Add all tool processes to the graph as OuterNodes
        for tool_id, tool in processes.items():
            if issubclass(type(tool), BaseCommandLineTool):
                tool = cast(BaseCommandLineTool, tool)
                node = OuterNode(tool_id)
                tool_id_to_node[tool_id] = node
                node.graph.add_nodes(ToolNode(tool))
                graph.add_nodes(node)

        # Add edges between nodes and their parents
        for tool_id, tool in processes.items():
            if issubclass(type(tool), BaseCommandLineTool):
                tool = cast(BaseCommandLineTool, tool)
                parents = [tool_id_to_node[p_id] for p_id in get_process_parents_ids(tool)]
                graph.add_parents(tool_id_to_node[tool_id], parents)    # type: ignore


    def optimize_dependency_graph(self) -> None:
        """
        TODO Description
        """
        # NOTE If we work with grouping, groups must be connected directly. 
        graph: OuterGraph = self.loading_context["graph"]
        graph.print()

        # NOTE just for testing purposes.
        graph.merge([id for id in graph.nodes])
        graph.print()
        for g in graph.nodes.values():
            g.graph.print()
        
    
    def build_step_namespace(
            self, 
            tool:  BaseCommandLineTool,
            runtime_context: Dict[str, Any],
        ) -> Dict[str, Any]:
        """
        """
        if tool.parent_workflow_id is None or tool.step_id is None:
            raise ValueError("Tool must have parent_process_id and step_id defined")
        # Add workflow inputs to namespace
        namespace = tool.build_base_namespace(runtime_context)

        # Add step inputs to namespace
        if tool.step_id is None:
            raise ValueError("tool.step_id cannot be None")
        
        workflow = self.loading_context["processes"][tool.parent_workflow_id]
        workflow = cast(BaseWorkflow, workflow)
        for in_id in workflow.steps[tool.step_id]["in"]:
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
            runtime_context: Dict[str, Value]
        ) -> Dict[str, Value]:
        """
        Create a new runtime context dictionary with updated source values
        from process input and step input valueFrom fields.
        """
        if tool.parent_workflow_id is None or tool.step_id is None:
            raise ValueError("Tool must have parent_process_id and step_id defined")
        parent_workflow_id = tool.parent_workflow_id
        parent_process = cast(BaseWorkflow, self.loading_context["processes"][parent_workflow_id])

        # Create a copy to prevent replacing source values with valueFrom values
        step_runtime_context: Dict[str, Value] = runtime_context.copy()

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
            # js2py returns lists and dicts as js2py.base.JsObjectWrapper.
            # How to handle this?
            if isinstance(expr_result, List):
                if len(expr_result) == 0:
                    # TODO List items can be any type, what to do?
                    # Probably just NoneType like below?            
                    value = Value([], NoneType, "null")
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


 
    def exec_outer_node(
            self,
            outer_node: OuterNode,
            runtime_context: Dict[str, Any],
            verbose: Optional[bool] = True,
            executor: Optional[ThreadPoolExecutor] = None
        ) -> Dict[str, Any]:
        """
        Execute the tasks of this node. 
        TODO OPTIMIZATION: OuterNodes are executed if all parent OuterNodes
             have finished. However if an OuterNode InnerGraph has multiple
             roots, some might be able to execute without all OuterNode parents
             having finished. Fix by checking for completed parents of each
             root in the OuterNode. Would need to collect and include data from
             parents that finish later into the runtime_context! 
        """
        graph: InnerGraph = outer_node.graph
        if graph is None:
            raise Exception("Node has no graph")

        if executor is None:
            executor = ThreadPoolExecutor()

        nodes = graph.nodes
        runnable_nodes: List[ToolNode] = graph.get_nodes(graph.roots) # type: ignore
        runnable: Dict[str, ToolNode] = {n.id: n for n in runnable_nodes}
        waiting: Dict[str, ToolNode] = {id: n for id, n in nodes.items() if id not in runnable}
        running: Dict[str, Tuple[Future, ToolNode]] = {}
        completed: Dict[str, ToolNode] = {}
        outputs: Dict[str, Value] = {}

        while len(runnable) != 0 or len(running) != 0:
            # Execute runnable nodes in the ThreadPool
            for node_id, node in runnable.copy().items():
                tool = node.tool
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
                    result: Dict[str, Value] = running_task[0].result()
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
            runtime_context: Dict[str, Any],
            verbose: bool = False,
            dask_client: Optional[Client] = None
        ) -> dict[str, Value]:
        """
        Execute the workflow as the main process. The workflow is executed by
        submitting each node of the workflow dependency graph to the scheduler
        as an asynchronous task. The workflow is executed in a polling
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
        graph: OuterGraph = self.loading_context["graph"]
        nodes = graph.nodes
        runnable_nodes: List[OuterNode] = graph.get_nodes(graph.roots) # type: ignore
        runnable: Dict[str, OuterNode] = {n.id: n for n in runnable_nodes}
        waiting: Dict[str, OuterNode] = {id: n for id, n in nodes.items() if id not in runnable}
        running: Dict[str, Tuple[Future, OuterNode]] = {}
        completed: Dict[str, OuterNode] = {}
        outputs: Dict[str, Value] = {}

        def lprint():
            s = ", "
            print("[WORKFLOW]: QUEUES")
            print(f"\twaiting: [{s.join([str(graph.short_ids[id]) for id in waiting.keys()])}]")
            print(f"\trunnable: [{s.join([str(graph.short_ids[id]) for id in runnable.keys()])}]")
            print(f"\trunning: [{s.join([str(graph.short_ids[id]) for id in running.keys()])}]")
            print(f"\tcompleted: [{s.join([str(graph.short_ids[id]) for id in completed.keys()])}]")
            print()

        if verbose:
            graph.print()
            print("\n".join([f"{short}: {id}" for id, short in graph.short_ids.items()]))
            lprint()

        # Polling loop that runs until all graph nodes have been executed.
        # Each node execution is submitted to the scheduler as an async
        # task. The polling loop checks for finished nodes and submits newly
        # executable nodes. Invalid workflows, faulty tools or bad input might
        # result in deadlocks, in which case an exception is raised.
        print()
        print("[WORKFLOW]: Executing workflow")
        while len(runnable) != 0 or len(running) != 0:
            # Execute runnable nodes
            for node_id, node in runnable.copy().items():
                if verbose: print("[WORKFLOW]: Submitting node", graph.short_ids[node_id])

                # If dask is disabled, the ThreadPoolExecutor client is used to
                # get concurrently executed tasks. If Dask is used, a 
                # ThreadPoolExecutor client is created on the execution node.
                future = client.submit(
                    self.exec_outer_node, 
                    node, 
                    runtime_context.copy(),
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
                    result: Dict[str, Value] = running_task[0].result()
                    outputs.update(result)
                    runtime_context.update(result)

                    # Move node to finished
                    completed[node_id] = running_task[1]
                    running.pop(node_id)
                    if verbose: lprint()

                    # Add new runnable nodes to queue by checking for each
                    # child if all its parents have completed.
                    for child_id in running_task[1].children:

                        # Only check children that have not already been queued
                        if child_id not in waiting:
                            continue

                        ready = True
                        for childs_parent_id in nodes[child_id].parents:
                            if childs_parent_id not in completed:
                                print("[WORKFLOW] Parent", {graph.short_ids[childs_parent_id]} , "not completed")
                                ready = False
                                break
                        if ready:
                            # All parents have finished: Queue up the child
                            runnable[child_id] = waiting.pop(child_id)
                    if verbose:
                        print("[WORKFLOW]: Completed node", graph.short_ids[running_task[1].id])
                        lprint()
            time.sleep(0.1)

        # Check for deadlock
        if len(runnable) == 0 and len(running) == 0 and len(waiting) != 0:
            s = "\n\t".join([node_id for node_id in waiting.keys()])
            raise Exception(f"Deadlock detected. Waiting nodes:\n\t{s}")
        
        # Workflow Processes do not exist in the dependency graph. The outputs
        # of the upper workflow must be sourced from tools instead by
        # traversing the subworkflows.
        new_outputs: Dict[str, Any] = {}
        for output_id, output_dict in self.outputs.items():
            step_id, step_out_id = output_dict["outputSource"].split("/")
            process = cast(BaseWorkflow, self.step_id_to_process[step_id])
            while issubclass(type(process), BaseWorkflow):
                process = cast(BaseWorkflow, process)
                step_id, step_out_id = process.outputs[step_out_id]["outputSource"].split("/")
                process = process.step_id_to_process[step_id]

            if not issubclass(type(process), BaseCommandLineTool):
                raise Exception()

            value = outputs[process.global_id(step_out_id)]
            new_outputs[self.global_id(output_id)] = value

        return new_outputs



##########################################################
#                         Node                          #
##########################################################
class BaseNode:
    id: str
    short_id: Optional[int]
    parents: Dict[str, Node]
    children: Dict[str, Node]

    def __init__(
            self,
            id: Optional[str] = None,
            short_id: Optional[int] = None,
            parents: Optional[List[Node]] = None,
            children: Optional[List[Node]] = None,
        ):
        self.id = id if id else str(uuid4())
        self.short_id = short_id if short_id else -1
        self.parents = {}
        self.add_parents(parents)
        self.children = {}
        self.add_children(children)


    def add_parents(
            self, 
            parents: Node | List[Node] | None
        ):
        """
        Add nodes as parents of this node.
        """
        if parents is None:
            return
        
        if isinstance(parents, Node):
            parents = [parents]
        
        self.parents.update({parent.id: parent for parent in parents})


    def add_children(
            self, 
            children: Node | List[Node] | None
        ):
        """
        Add nodes as children of this node.
        """
        if children is None:
            return
        
        if isinstance(children, Node):
            children = [children]
        
        self.children.update({child.id: child for child in children})


    def is_root(self) -> bool:
        return len(self.parents) == 0

    def is_leaf(self) -> bool:
        return len(self.parents) == 0


class ToolNode(BaseNode):
    tool: BaseCommandLineTool
    parents: Dict[str, ToolNode]
    children: Dict[str, ToolNode]
    
    def __init__(
            self,
            tool: BaseCommandLineTool,
            short_id: Optional[int] = None,
            parents: Optional[List[ToolNode]] = None,
            children: Optional[List[ToolNode]] = None,
        ):
        """
        ``InnerNode``s are contained in an ``InnerGraph`` and are essentially
        command-line tool Process wrappers.
        """
        super().__init__(tool.id, short_id, parents, children) # type: ignore
        self.tool = tool


class OuterNode(BaseNode):
    graph: InnerGraph
    parents: Dict[str, OuterNode]
    children: Dict[str, OuterNode]

    @property
    def tool_ids(self) -> List[str]:
        return list(self.graph.nodes)

    def __init__(
            self,
            id: Optional[str] = None,
            short_id: Optional[int] = None,
            parents: Optional[List[OuterNode]] = None,
            children: Optional[List[OuterNode]] = None,
        ):
        """
        ``OuterNode``s are contained in an ``OuterGraph``, containing graphs
        (``InnerGraph``) themselves. 
        """
        super().__init__(id, short_id, parents, children) # type: ignore
        self.graph = InnerGraph()


# Typedef
Node = ToolNode | OuterNode


##########################################################
#                         Graph                          #
##########################################################
class BaseGraph:
    size:      int
    nodes:     Dict[str, Node]
    roots:     List[str]
    leaves:    List[str]
    next_short_id:   int
    short_ids: Dict[str, int]
    in_edges:  Dict[str, List[str]]
    out_edges: Dict[str, List[str]]

    def __init__(self):
        """
        New nodes and edges MUST be added to the graph with the ``add_nodes()``
        and ``add_edges()`` methods.
        """
        self.size = 0
        self.nodes = {}
        self.roots = []
        self.leaves = []
        self.next_short_id = 0
        self.short_ids = {}
        self.in_edges = {}
        self.out_edges = {}


    def add_nodes(self, nodes: Node | List[Node]) -> None:
        """
        Add one or more nodes to the graph.
        NOTE: This only adds a node. For edges, see ``add_edges()``.
        """
        if isinstance(nodes, Node):
            nodes = [nodes]
        for node in nodes:
            if node.id in self.nodes:
                raise Exception(f"Graph already contains a node with id {node.id}")
            self.nodes[node.id] = node
            self.roots.append(node.id)
            self.leaves.append(node.id)
            self.short_ids[node.id] = self.next_short_id
            node.short_id = self.next_short_id
            self.next_short_id += 1
            self.size += 1


    def add_edges(
            self, 
            di_edges: List[Tuple[Node, Node]]
        ):
        """
        Add directed edges between nodes. Dependencies are registered in both
        the child and the parent nodes, e.g. child and parent nodes have
        references to each other.
        """
        for node_a, node_b in di_edges:
            # Register edges in graph
            if node_a.id in self.out_edges:
                # Ignore duplicates
                if not node_b.id in self.out_edges[node_a.id]:
                    self.out_edges[node_a.id].append(node_b.id)
            else:
                self.out_edges[node_a.id] = [node_b.id]

            if node_b.id in self.in_edges:
                # Ignore duplicates
                if node_a.id in self.in_edges[node_b.id]:
                    self.in_edges[node_b.id].append(node_a.id)
            else:
                self.in_edges[node_b.id] = [node_a.id]

            # Register edges in nodes
            node_a.add_children(node_b)
            node_b.add_parents(node_a)

            # Update roots and leaves
            if node_a.id in self.leaves:
                self.leaves.remove(node_a.id)
            if node_b.id in self.roots:
                self.roots.remove(node_b.id)
            if node_a.is_root() and node_a.id not in self.roots:
                self.roots.append(node_a.id)
            if node_b.is_leaf() and node_b.id not in self.roots:
                self.roots.append(node_b.id)


    def add_parents(
            self,
            node: Node | str,
            parents: List[Node | str]
        ) -> None:
        """
        Add parents to the child node and add the child node as the child of
        each parent node.
        """
        if isinstance(node, str):
            node = self.get_nodes(node)[0]

        edges: List[Tuple[Node, Node]] = []
        for parent in parents:
            if isinstance(parent, Node):
                edges.append((parent, node))
            elif isinstance(parent, str):
                edges.append((self.nodes[parent], node))
            else:
                raise TypeError(f"Expected 'str' or 'Node', but found '{type(parent)}'")
        self.add_edges(edges)


    def add_children(
            self,
            node: Node,
            children: List[Node]
        ) -> None:
        """
        Add children to the parent node and add the parent node as the parent
        of each child.        
        """
        edges: List[Tuple[Node, Node]] = []
        for child in children:
            if isinstance(child, Node):
                edges.append((node, child))
            else:
                raise TypeError(f"Expected 'Node', but found '{type(child)}'")
        self.add_edges(edges)

    
    def get_nodes(self, node_ids: str | List[str]) -> List[Node]:
        if isinstance(node_ids, str):
            return [self.nodes[node_ids]]
        return [self.nodes[id] for id in node_ids]

    
    def remove_nodes(
            self,
            targets: Node | List[Node]
        ):
        """
        Remove ``target`` nodes and related edges from the graph. 
        """
        if isinstance(targets, Node):
            targets = [targets]
        
        target_ids = [t.id for t in targets]
        for id in target_ids:
            self.size -= 1
            self.nodes.pop(id, None)
            if id in self.roots: self.roots.remove(id)
            if id in self.leaves: self.leaves.remove(id)
            self.in_edges.pop(id, None)
            for parents in self.in_edges.values(): 
                if id in parents: parents.remove(id)
            self.out_edges.pop(id, None)
            for children in self.out_edges.values():
                if id in children: children.remove(id)


    def __str__(self) -> str:
        """
        Construct a string containing graph info and return it. Simple node
        IDs are used to improve clarity. Node IDs are mapped to simple node
        IDs in self.short_ids.

        Returns a string containing:
            - Root nodes
            - Edges
            - Leaf nodes
        """
        s = "nodes[parents/children]: "
        for node_id in self.nodes:
            s += f"{self.short_ids[node_id]}["
            if node_id in self.in_edges:
                s += f"{','.join([str(self.short_ids[p_id]) for p_id in self.in_edges[node_id]])}"
            else:
                s += "."
            s += "/"
            if node_id in self.out_edges:
                s += f"{','.join([str(self.short_ids[c_id]) for c_id in self.out_edges[node_id]])}"
            else:
                s += "."
            s += "] "

        s += "\nroots: " 
        for root_id in self.roots:
            s += f"{self.short_ids[root_id]} "

        s += "\nedges: \n"
        for node_id, child_id in self.out_edges.items():
            for child in child_id:
                s += f"\t{self.short_ids[node_id]} -> {self.short_ids[child]}\n"

        s += "leaves: " 
        for leaf_id in self.leaves:
            s += f"{self.short_ids[leaf_id]} "
        return s

    
    def print(self) -> None:
        """ Print graph information. """
        print()
        print(self)
        print()


class OuterGraph(BaseGraph):
    nodes: Dict[str, OuterNode]

    def merge_inner_graphs(
            self,
            inner_graphs: List[InnerGraph]
        ) -> InnerGraph:
        """
        Create a new InnerGraph object and merge the graphs in ``inner_graphs``
        into it.

        Returns:
            The newly created InnerGraph
        """
        inner = InnerGraph()
        for g in inner_graphs:
            inner.add_nodes(list(g.nodes.values()))

            # Every directed edge between nodes is both an in- and out-edge, so
            # using either the in- or out-edges suffices.
            for src, children in g.out_edges.items():
                src_node = g.get_nodes(src)[0]
                inner.add_children(src_node, g.get_nodes(children))

        # If a tool is added to a merged graph with a parent tool that was
        # previously not in the same graph, a new edge needs to be created
        # between the tools.
        # NOTE: add_parents() ignores duplicate edges
        for tool_node in inner.nodes.values():
            tool = tool_node.tool
            parent_ids = get_process_parents_ids(tool)
            for parent_id in parent_ids:
                if parent_id in inner.nodes:
                    inner.add_parents(tool_node, [parent_id])

        return inner


    def merge(
            self,
            nodes_or_ids: List[OuterNode | str]
        ) -> str:
        """
        Merge a number of ``OuterNodes`` into a new ``OuterNode`` by combining
        their ``parents``, ``children``, and ``InnerGraph``'s. The new
        ``OuterNode`` replaces the merged ``nodes``.

        Returns the ``id`` of the newly created node.
        """
        if len(nodes_or_ids) < 2:
            raise Exception("Merging needs atleast 2 nodes")
        
        nodes: List[OuterNode] = []
        node_ids: List[str] = []
        for n in nodes_or_ids:
            if isinstance(n, str):
                nodes.append(self.nodes[n])
                node_ids.append(n)
            else:
                nodes.append(n)
                node_ids.append(n.id)

        ids: List[str] = []
        parents: List[OuterNode] = []
        children: List[OuterNode] = []
        inner_graphs: List[InnerGraph] = []
        for node in nodes:
            ids.append(str(node.short_id))
            parents.extend([v for k, v in node.parents.items()
                              if k not in node_ids])
            children.extend([v for k, v in node.children.items()
                              if k not in node_ids])
            inner_graphs.append(node.graph)

        new_id = ":".join(ids)
        new_node = OuterNode(id = new_id,
                             parents = parents,
                             children = children)
        new_node.graph = self.merge_inner_graphs(inner_graphs)
        edges = []
        for src, ps in self.in_edges.items():
            if src in node_ids:
                edges.extend([(self.nodes[src], self.nodes[p]) for p in ps])
        for src, cs in self.out_edges.items():
            if src in node_ids:
                edges.extend([(self.nodes[src], self.nodes[c]) for c in cs])

        self.add_nodes(new_node)
        self.add_edges(edges)
        self.remove_nodes(nodes)    # type: ignore
        return new_node.id


class InnerGraph(BaseGraph):
    nodes: Dict[str, ToolNode]