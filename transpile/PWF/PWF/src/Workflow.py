import dask.delayed
import importlib
import inspect
import sys

from abc import abstractmethod
from copy import deepcopy
from dask.delayed import Delayed
from pathlib import Path
from typing import Optional, Tuple, Union

from .CommandLineTool import BaseCommandLineTool
from .Process import BaseProcess, Graph, Node
from .utils import Absent

class BaseWorkflow(BaseProcess):
    def __init__(
            self,
            main: bool = False,
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
        self.steps: dict[str, dict[str, str]] = {}

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
            self.create_task_graph()
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


    def _process_steps(self) -> None:
        """
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

        # Recursively load all processes
        print("Loading process files:")
        for step_id, step_dict in self.steps.items():
            step_process = self._load_process_from_uri(step_dict["run"], step_id)
            processes[step_process.id] = step_process
            self.step_id_to_process[step_id] = step_process
            node = Node(id = step_process.id, processes = [step_process])
            graph.register_node(node)

        # Add tool nodes to the dependency graph
        for tool in processes.values():
            if issubclass(type(tool), BaseCommandLineTool):
                graph.connect_node(
                    node_id = tool.id,
                    parents = get_process_parents(tool),
                    # children = 
                )

        # TODO remove
        # graph.print()


    def optimize_dependency_graph(self) -> None:
        """
        TODO Description
        """
        pass


    def create_task_graph(self) -> None:
        """
        TODO Description
        """
        graph: Graph = self.loading_context["graph"]
        
        # 
        id_to_delayed: dict[str, Delayed] = {}
        visited: list[str] = []
        frontier: list[str] = deepcopy(graph.roots)
        while len(frontier) != 0:
            node_id = frontier.pop(0)
            node = graph.nodes[node_id]

            # Check if parents have been visited
            good = True
            for parent in node.parents:
                # If not all parents have been visited, visit later
                if parent not in visited:
                    good = False
                    frontier.append(node.id)
                    break

            if not good: 
                continue

            parents: list[Delayed] = []
            for parent_node_id in node.parents:
                parents.append(id_to_delayed[parent_node_id])

            # Create and register dask.Delayed tool wrapper
            # FIXME This works only for nodes with a single process
            node.processes[0].create_task_graph(*parents)
            id_to_delayed[node_id] = node.processes[0].task_graph_ref
            visited.append(node_id)

            # Add children to frontier
            for child_node_id in node.children:
                if child_node_id not in frontier:
                    frontier.append(child_node_id)

        if len(visited) != graph.size:
            raise Exception("Dangling nodes: Not all nodes have been visited")

        def knot(*parents):
            """ Tie together all leaves to ensure they are executed """
            # print("Finished executing task tree")
            pass

        leaves: list[Delayed] = []
        for node_id in graph.leaves:
            # FIXME This works only for nodes with a single process
            leaves.append(graph.nodes[node_id].processes[0].task_graph_ref)

        self.task_graph_ref = dask.delayed(knot)(leaves)
        self.task_graph_ref.visualize(filename="graph.svg")


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
                parent_id = self.id,
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
        elif "default" in step_dict["in"][input_id]:
            return True, None
        raise NotImplementedError()
    
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