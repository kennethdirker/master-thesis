import importlib
import inspect
import sys

from abc import abstractmethod
from pathlib import Path
from typing import Optional, Tuple, Union

# from .CommandLineTool import BaseCommandLineTool
from .Process import BaseProcess, Graph, Node
# from .utils import Graph, Node

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
        self.create_dependency_graph()

        # Only the main process executes the workflow.
        if main:
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
    

    # def get_process_parents(self) -> list[str]:
    #     """
    #       NOTE: Relocated in CommandLineTool 
    #     TODO Description
    #     """
    #     if self.is_root:
    #         return []
        
    #     processes: dict[str, BaseProcess] = self.loading_context["processes"]        
    #     parents = []

    #     for input_id in self.inputs:
    #         # NOTE Make sure this still works when not working with BaseWorkflow
    #         process = processes[self.parent_id] # BaseWorkflow
    #         step_id = self.step_id
    #         step_dict = process.steps_dict[step_id]
    #         # FIXME support other sources, like default
    #         source = step_dict["in"][input_id]["source"]
            
    #         # Go up the process tree until a tool is encountered
    #         while True:
    #             if "/" in source:
    #                 # Other step of this process is the input source
    #                 process = process.step_to_process[step_id]
    #                 parents.append(process._id)
    #                 break
    #             else:
    #                 # Parent of this process is the input source
    #                 if process.is_root():
    #                     # Input comes from input object
    #                     break
    #                 else:
    #                     # Input comes from another source
    #                     step_id = process.step_id
    #                     process = processes[process.parent_id]
    #                     step_dict = process.steps_dict[step_id]
    #                     # FIXME support other sources, like default
    #                     source = step_dict["in"][input_id]["source"]
    #     print(parents)
    #     return parents

    
    def create_dependency_graph(self) -> None:
        """ 
        TODO Description
        FIXME Not accurate anymore
        Create a Dask task graph with the sub-processes of this workflow. 
        Can be overwritten to alter execution behaviour. 
        """
        processes = self.loading_context["processes"]
        graph: Graph = self.loading_context["graph"]
        # inputs = self.loading_context["inputs"]

        # TODO Load and map all processes
        for step_id, step_dict in self.steps_dict.items():
            step_process = self._load_single_class_from_uri(step_dict["run"], step_id)
            processes[step_process._id] = step_process
            self.step_to_process[step_id] = step_process
        print(graph.leaves)        
        # TODO Map inputs and outputs
        
        
        # TODO Map step in/out to inputs/outputs

        # NOTE Tool should add the node itself
        # # TODO For each Tool
        #     # TODO Get input dependencies
        #     # TODO Create Node
        #     # TODO Add Node to Graph
        # for proc_id, process in processes.items():
        #     if issubclass(type(process), BaseWorkflow):
        #         continue
    
        #     node = Node(
        #         id = proc_id,
        #         parents = self.get_process_parents(),
        #         # children = self.get_tool_children(),
        #         processes = [process]
        #     )
        #     graph.add_node(node)


        # The step register uses the step id combined with the
        # ID of the current workflow process layer to allow duplicate
        # step IDs across sub-workflows.
        # step_register: dict[str, Tuple[dict, BaseProcess]] = {} # {step_id: (step_dict, Process)}
        # self.loading_context["step_register"]
        # Create a Process for each step and cache in step register
        # print("Loading steps...")
        # for step_id, step_dict in self.steps_dict.items():
            # step_process = self._load_single_class_from_uri(step_dict["run"])
            # TODO: Is this how we want global ids to work?
            # self.step_register[self._id + ":" + step_id] = (step_dict, step_process)

        # Get process dependencies
        # for step_id, [step_dict, step_process] in step_register.items():
        #     print("\n", step_id, step_dict, step_process._id, "\n")

        #     # Check inputs
        #     for step_in_id, step_in_dict in step_dict["in"].items():
        #         if "source" in step_in_dict and "/" in step_in_dict["source"]:
        #             # This step depends on the output of another step
        #             parent_step_id, parent_output_id = step_in_dict["source"].split("/")
        #             o = step_register[parent_step_id]
        #             print(o)
        #             self.parents.append(o[1]._id)
        # print("\n", self.parents, "\n")

        # # Get handles from global loading context
        # graph: Graph = self.loading_context["graph"]
        # process_register = self.loading_context["process_register"]

        # # Construct dependency graph
        # for step_id, step_dict in self.steps_dict.items():
        #     # Each Process recursively adds nodes to the global dependency graph.
        #     step_process = self._load_single_class_from_uri(step_dict["run"])


        #     if issubclass(type(step_process), BaseWorkflow):
        #         # Register inputs and outputs
        #         pass
        #     elif issubclass(type(step_process), BaseCommandLineTool):
        #         # Add tool nodes to graph
        #         process_register[self.global_id(step_id)] = step_process
        #         node = Node(
        #             id = step_process._id,
        #             parents = self.get_,
        #             processes = [step_process]
        #         )
        #         graph.add_node(node)

            
        # TODO
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
    

    def _load_single_class_from_uri(
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

