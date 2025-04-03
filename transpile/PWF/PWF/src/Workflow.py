import importlib
import inspect
import sys

from abc import abstractmethod
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
        # inputs = self.loading_context["inputs"]

        # Recursively load all processes
        for step_id, step_dict in self.steps_dict.items():
            print('step id: ', step_id)
            step_process = self._load_process_from_uri(step_dict["run"], step_id)
            processes[step_process._id] = step_process
            self.step_to_process[step_id] = step_process

        # Add tool nodes to the dependency graph
        for tool in processes.values():
            if issubclass(type(tool), BaseCommandLineTool):
                # tool.create_dependency_graph()
                node = Node(
                    id = tool._id,
                    parents = get_process_parents(tool),
                    processes = [tool]
                )
                graph.add_node(node)
        print(graph)

            
        raise NotImplementedError
    

    def optimize_dependency_graph(self) -> None:
        """
        TODO Description
        """
        pass


    def create_task_graph(self) -> None:
        """
        TODO Description
        """
        # graph: Graph = self.loading_context["graph"]
        # graph.tie_leaves()
        # self.task_graph_ref = ...
        raise NotImplementedError
    

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
    """
    if tool.is_root:
        return []
    
    processes: dict[str, BaseProcess] = tool.loading_context["processes"]
    parents = []
    for input_id in tool.inputs_dict:
        # Start in the parent of tool
        # NOTE Make sure this still works when not working with BaseWorkflow
        process: BaseWorkflow = processes[tool.parent_id] 
        step_id = tool.step_id
        step_dict = process.steps_dict[step_id]

        # FIXME support other sources, like default
        if "source" in step_dict["in"][input_id]:
            source = step_dict["in"][input_id]["source"]
        elif "default" in step_dict["in"][input_id]:
            continue
        else:
            raise NotImplementedError()
        
        
        # Go up the process tree, until a tool or the input object
        # is encountered
        while True:
            print()
            print(process._id)
            print(step_id)
            print(source)
            print()
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
                    # FIXME support other sources, like default
                    source = step_dict["in"][input_id]["source"]
                    if "source" in step_dict["in"][input_id]:
                        source = step_dict["in"][input_id]["source"]
                    elif "default" in step_dict["in"][input_id]:
                        break
                    else:
                        raise NotImplementedError()
    return parents