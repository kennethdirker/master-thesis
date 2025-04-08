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

class BaseWorkflow(BaseProcess):
    def __init__(
            self,
            main: bool = False,
            runtime_context: Optional[dict] = None,
            loading_context: Optional[dict[str, str]] = None,
            parent_id: Optional[str] = None,
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
            parent_id = parent_id,
            step_id = step_id
        )

        # TODO Decide if this is the way, see self.groups()
        # self.groupings = ...

        # FIXME
        # Mapping of step ins to their sources 
        self.in_to_source: dict[str, str] = {}
        self.step_to_process: dict[str, str] = {}

        # Must be overridden by self.steps().
        self.steps_dict: dict[str, dict[str, str]] = {}

        # Digest workflow file
        self.metadata()
        self.inputs()
        self._process_inputs()
        self.outputs()
        self.requirements()
        self.steps()

        # Only the main process executes the workflow.
        if main:
            self.create_dependency_graph()
            self.optimize_dependency_graph()
            self.create_task_graph()
            self.execute()


    @abstractmethod
    def steps(self):
        """
        Defines the sub-processes of this workflow. Overwriting this function
        is mandatory for workflows 
        """
        # Example:
        # self.steps_dict = {
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

    
    def groups(self) -> None:
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
        for step_id, step_dict in self.steps_dict.items():
            step_process = self._load_process_from_uri(step_dict["run"], step_id)
            processes[step_process._id] = step_process
            self.step_to_process[step_id] = step_process
            node = Node(id = step_process._id, processes = [step_process])
            graph.register_node(node)

        # Add tool nodes to the dependency graph
        for tool in processes.values():
            if issubclass(type(tool), BaseCommandLineTool):
                graph.connect_node(
                    node_id = tool._id,
                    parents = get_process_parents(tool),
                    # children = 
                )

        # TODO remove
        print(graph)


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
        frontier: list[str] = deepcopy(graph.roots)
        visited: list[str] = []
        id_to_delayed: dict[str, Delayed] = {}
        

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

            # Create and register dask.Delayed wrapper
            node.processes[0].create_task_graph(*parents)
            id_to_delayed[node_id] = node.processes[0].task_graph_ref
            visited.append(node_id)

            # Add children to frontier
            for child_node_id in node.children:
                frontier.append(child_node_id)



        if len(visited) != graph.size:
            raise Exception("Dangling nodes: Not all nodes have been visited")

        def knot(*parents):
            """ Tie together all leaves to ensure they are executed """
            print("Finished executing task tree")

        leaves: list[Delayed] = []
        for node_id in graph.leaves:
            # FIXME This works only for nodes with a single process
            leaves.append(graph.nodes[node_id].processes[0].task_graph_ref)

        self.task_graph_ref = dask.delayed(knot)(leaves)
        self.task_graph_ref.visualize(filename="graph.svg")
    

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
                parent_id = self._id,
                step_id = step_id
            )
        raise Exception(f"{uri} does not contain a BaseProcess subclass")


def get_process_parents(tool: BaseCommandLineTool) -> list[str]:
    """
    TODO Description
    NOTE: How to implement optional args?
    """
    if tool.is_root:
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

    for input_id in tool.inputs_dict:
        # Start in the parent of tool
        # NOTE Make sure this still works when not working with BaseWorkflow
        process: BaseWorkflow = processes[tool.parent_id] 
        step_id = tool.step_id
        step_dict = process.steps_dict[step_id]
        cont, source = get_source(step_dict, input_id)
        if cont:
            continue
        
        # Go up the process tree, until a tool or the input object
        # is encountered
        while True:
            if "/" in source:
                # A step of this process is the input source
                parent_step_id, _ = source.split("/")
                process = process.step_to_process[parent_step_id]
                parents.append(process._id)
                break
            else:
                # Parent of this process is the input source
                if process.is_root:
                    # Input comes from input object
                    break
                else:
                    # Input comes from another source upstream
                    process = processes[process.parent_id]
                    step_id = process.step_id
                    step_dict = process.steps_dict[step_id]
                    cont, source = get_source(step_dict, input_id)
                    if cont:
                        break
    return parents