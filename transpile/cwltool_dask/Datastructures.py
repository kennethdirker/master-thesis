# Standard modules
from typing import Dict, List, Any, Union
from pathlib import Path
import copy
import cwl_utils.parser.cwl_v1_2 as cwl

# TODO change to use cwl_utils
class DirType(Path):
    def __init__(self, *args, **kwargs):
        super.__init__(self, *args, **kwargs)
        self.path_type = "dir"

    def get_type(self):
        return self.path_type

# TODO change to use cwl_utils
class FileType(Path):
    def __init__(self, *args, **kwargs):
        super.__init__(self, *args, **kwargs)
        self.path_type = "file"

    def get_type(self):
        return self.path_type

# TODO change to use cwl_utils
InputTypes = Union[
    bool,
    str,
    int,
    float,
    DirType,
    FileType,
]

# TODO change to use cwl_utils
OutputTypes = Union[
    bool,
    str,
    int,
    float,
    DirType,
    FileType,
]

class Step:
    """"""
    # NOTE: Turn into Enum? Example:
    # from enum import Enum
    # class StepAttrs(Enum):
    #     cwlVersion = 1
    #     id = 2
    #     baseCommand = 3
    
    ATTR_NAMES = [
        "class_",
        "cwlVersion",
        "id",
        "baseCommand",
        "arguments",
        "inputs",
        "outputs",
        "requirements",
        "hints",
        "stderr",
        "stdout",
        "stdin",
        "successCodes",
        "permanentFailCodes",
        "temporaryFailCodes",
        "save",
        "extension_fields",
        "intent",
        "label",
        "doc",
    ]

    def __init__(
            self,
            # id: str,
            # NOTE: baseCommand is optional and contain empty list, does this happen in LINQ? 
            # baseCommand: str | list[str] | None,
            # inputs: list[cwl.CommandInputParameter],
            # outputs: list[cwl.CommandOutputParameter],
            **kwargs
        ):
        # self.id: str = id
        # NOTE: baseCommand is optional ('arguments' is then used), does this happen in LINQ? 
        # self.baseCommand = baseCommand
        # self.inputs = inputs
        # self.outputs = outputs
        # self.command_template = ""  #NOTE: Will hold template used to substitute step inputs
        # self.attrs: list[str] = [
        #     "id",
        #     "baseCommand",
        #     "template",
        #     "inputs",
        #     "outputs",
        # ]     # Keep track of used CWL step fields
        self.attrs: list[str] = []
        # self.add_attrs(**kwargs)


    # def add_attrs(self, **kwargs):
        """"""
        for attr, value in kwargs.items():
            if attr in self.ATTR_NAMES:
                setattr(self, attr, value)

                # Note down new attributes
                if attr not in self.attrs:
                    self.attrs.append(attr)
            else:
                # TODO Decide if debugging only
                raise Exception(f"{attr} is not a valid step attribute.")


    # def _f1(self, template: str) -> str:
    #     """
    #     Collect CommandLineBinding objects from arguments. Assign a 
    #     sorting key [position, i] where position is 
    #     CommandLineBinding.position and i is the index in the arguments 
    #     list.

    #     https://www.commonwl.org/v1.2/CommandLineTool.html#CommandLineBinding

    #     When listed under inputBinding in the input schema, the term "value" 
    #     refers to the corresponding value in the input object. For binding 
    #     objects listed in CommandLineTool.arguments, the term "value" refers 
    #     to the effective value after evaluating valueFrom.
    #     The binding behavior when building the command line depends on the 
    #     data type of the value. If there is a mismatch between the type 
    #     described by the input schema and the effective value, such as 
    #     resulting from an expression evaluation, an implementation must use 
    #     the data type of the effective value.
    #         string: Add prefix and the string to the command line.
    #         number: Add prefix and decimal representation to command line.
    #         boolean: If true, add prefix to the command line. 
    #                 If false, add nothing.
    #         File: Add prefix and the value of File.path to the command line.
    #         Directory: Add prefix and the value of Directory.path to the 
    #                 command line.
    #         array: If itemSeparator is specified, add prefix and the join the 
    #                 array into a single string with itemSeparator separating 
    #                 the items. Otherwise, first add prefix, then recursively 
    #                 process individual elements. If the array is empty, it 
    #                 does not add anything to command line.
    #         object: Add prefix only, and recursively add object fields for 
    #                 which inputBinding is specified.
    #         null: Add nothing.
    #     """
    #     return "placeholder"


    # def _f2(self, template: str) -> str:
    #     """
    #     Collect CommandLineBinding objects from the inputs schema and 
    #     associate them with values from the input object. Where the 
    #     input type is a record, array, or map, recursively walk the 
    #     schema and input object, collecting nested `CommandLineBinding` 
    #     objects and associating them with values from the input object.

    #     https://www.commonwl.org/v1.2/CommandLineTool.html#CommandLineBinding
        
    #     When listed under inputBinding in the input schema, the term "value" 
    #     refers to the corresponding value in the input object. For binding 
    #     objects listed in CommandLineTool.arguments, the term "value" refers 
    #     to the effective value after evaluating valueFrom.
    #     The binding behavior when building the command line depends on the 
    #     data type of the value. If there is a mismatch between the type 
    #     described by the input schema and the effective value, such as 
    #     resulting from an expression evaluation, an implementation must use 
    #     the data type of the effective value.
    #         string: Add prefix and the string to the command line.
    #         number: Add prefix and decimal representation to command line.
    #         boolean: If true, add prefix to the command line. 
    #                 If false, add nothing.
    #         File: Add prefix and the value of File.path to the command line.
    #         Directory: Add prefix and the value of Directory.path to the 
    #                 command line.
    #         array: If itemSeparator is specified, add prefix and the join the 
    #                 array into a single string with itemSeparator separating 
    #                 the items. Otherwise, first add prefix, then recursively 
    #                 process individual elements. If the array is empty, it 
    #                 does not add anything to command line.
    #         object: Add prefix only, and recursively add object fields for 
    #                 which inputBinding is specified.
    #         null: Add nothing.
    #     """
    #     return "placeholder"


    # def _f3(self, template: str) -> str:
    #     """
    #     Create a sorting key by taking the value of the position field 
    #     at each level leading to each leaf binding object. If position 
    #     is not specified, it is not added to the sorting key. For 
    #     bindings on arrays and maps, the sorting key must include the 
    #     array index or map key following the position. If and only if 
    #     two bindings have the same sort key, the tie must be broken 
    #     using the ordering of the field or parameter name immediately 
    #     containing the leaf binding.
    #     """
    #     return "placeholder"


    # def _f4(self, template: str) -> str:
    #     """
    #     Sort elements using the assigned sorting keys. Numeric entries 
    #     sort before strings.
    #     """
    #     return "placeholder"


    # def _f5(self, template: str) -> str:
    #     """
    #     In the sorted order, apply the rules defined in CommandLineBinding 
    #     to convert bindings to actual command line elements.
    #     """
    #     return "placeholder"


    # def _f6(self, template: str) -> str:
    #     """ 
    #     Insert elements from baseCommand at the beginning of the template. 
    #     """
    #     if hasattr(self, "baseCommand"):
    #         return ' '.join([*self.baseCommand, template])
    #     else:
    #         # TODO: 'baseCommand' is empty? Look at 'arguments' instead.
    #         raise NotImplementedError("'baseCommand' is empty, 'arguments' should be used")

    
    # def build_command_template(self, input_object):
    #     """
    #     From https://www.commonwl.org/v1.2/CommandLineTool.html#Input_binding:

    #     The tool command line is built by applying command line bindings to 
    #     the input object. Bindings are listed either as part of an input 
    #     parameter using the inputBinding field, or separately using the 
    #     arguments field of the CommandLineTool.

    #     The algorithm to build the command line is as follows. In this 
    #     algorithm, the sort key is a list consisting of one or more numeric or 
    #     string elements. Strings are sorted lexicographically based on UTF-8 
    #     encoding.
    #         1.  Collect CommandLineBinding objects from arguments. Assign a 
    #             sorting key [position, i] where position is 
    #             CommandLineBinding.position and i is the index in the arguments 
    #             list.
    #         2.  Collect CommandLineBinding objects from the inputs schema and 
    #             associate them with values from the input object. Where the 
    #             input type is a record, array, or map, recursively walk the 
    #             schema and input object, collecting nested `CommandLineBinding` 
    #             objects and associating them with values from the input object.
    #         3.  Create a sorting key by taking the value of the position field 
    #             at each level leading to each leaf binding object. If position 
    #             is not specified, it is not added to the sorting key. For 
    #             bindings on arrays and maps, the sorting key must include the 
    #             array index or map key following the position. If and only if 
    #             two bindings have the same sort key, the tie must be broken 
    #             using the ordering of the field or parameter name immediately 
    #             containing the leaf binding.
    #         4.  Sort elements using the assigned sorting keys. Numeric entries 
    #             sort before strings.
    #         5.  In the sorted order, apply the rules defined in CommandLineBinding 
    #             to convert bindings to actual command line elements.
    #         6.  Insert elements from baseCommand at the beginning of the command 
    #             line.
    #     """
    #     positions: list[(int, int, cwl.CommandLineBinding)]
    #     template = self._f1(positions)
    #     template = self._f2(template, positions)
    #     template = self._f3(template, positions)
    #     template = self._f4(template)
    #     template = self._f5(template)
    #     template = self._f6(template)
    #     self.command_template = template


class Node:
    def __init__(
            self, 
            node_id: int,
            parents: list[int],
            steps: Step | list[Step],
            dependencies: dict[int, int | list[int]] | None = None
        ):
        """
        Arguments
            node_id
                Node identifier.
                Not to be confused with the 'id' field of a CWL file.
            parents
                Parent node(s) in the Node graph. 
                Empty list if this node is root.
            steps
                
            dependencies
                Mapping of directed dependencies between steps.
        """
        self.id = node_id
        self.parents: list[int] = parents
        
        if   isinstance(steps, list) and len(steps) > 1:
            # Grouped steps in node
            self.is_grouped = True
            self.steps: list[Step] = steps
            self.dependencies: dict[int, int] = dependencies
        elif (isinstance(steps, list) and len(steps) == 1) \
          or isinstance(steps, Step):
            # Single step in node
            self.steps: list[Step] = [steps]
            self.is_grouped = False
        else:
            raise TypeError(f"Invalid type: {type(steps)}")


class Graph:
    def __init__(
            self, 
            # grouping: bool = False
        ):
        self.roots: list[int] = []
        self.nodes: dict[int, Node] = {}
        self.dependencies = {}  # {child_id: [parent_ids], ...}
        # self.grouping: bool = grouping


    def add_node(
            self,
            node: Node,
            parents: int | list[int] | None = None
        ):
        """
        Arguments
            node
                Node to add to graph.
            parents
                IDs of nodes that need to be executed before the added node.

        """
        if node.id in self.nodes:
            raise Exception(f"Node ID already exists in graph. Invalid ID: {node.id}")

        if parents is None \
            or (isinstance(parents, list) and len(parents) == 0):
            # Insert root node
            self.roots.append(node.id)
            self.nodes[node.id] = node
        elif isinstance(parents, int) or isinstance(parents, list):
            if isinstance(parents, int):
                parents = [parents]
            self.dependencies[node.id] = parents
            self.nodes[node.id] = node
        else:
            raise Exception(f"Invalid parameter type: {type(parents)}")
            
