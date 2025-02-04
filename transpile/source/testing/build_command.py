from typing import List, Dict, Any, Union
"""


Mistral AI's answer to implementing command line building.


Explanation:
1.  CommandLineBinding Class: Represents a command-line binding with optional position, prefix, and separate attributes.
2.  CommandLineTool Class: Represents a CWL CommandLineTool with baseCommand, inputs, and arguments.
3.  build_command_line Function:
    -   collect_bindings: Recursively collects command-line bindings from the inputs schema and associates them with values from the input object.
    -   apply_binding: Converts a binding to actual command-line elements based on the binding rules.
    -   argument_bindings: Collects bindings from the arguments field.
    -   input_bindings: Collects bindings from the inputs schema.
    -   all_bindings: Combines and sorts all bindings.
    -   command_line: Builds the command line by applying the bindings in the sorted order and inserting the baseCommand at the beginning.
This function follows the specified algorithm to build the command line for a CWL CommandLineTool.


"""

# Define a class to represent command-line bindings
class CommandLineBinding:
    def __init__(self, position: Union[int, None] = None, prefix: str = "", separate: bool = True):
        self.position = position
        self.prefix = prefix
        self.separate = separate

# Define a class to represent a CWL CommandLineTool
class CommandLineTool:
    def __init__(self, baseCommand: List[str], inputs: Dict[str, Any], arguments: List[CommandLineBinding]):
        self.baseCommand = baseCommand
        self.inputs = inputs
        self.arguments = arguments

# tool: Object storing CWL CommandLineTool schema
# input_object: Dict containing the inputs for filling in the command line parameters
def build_command_line(tool: CommandLineTool, input_object: Dict[str, Any]) -> List[str]:
    # Nested function to recursively collect command-line binding from the inputs schema
    def collect_bindings(schema, input_obj, prefix_key=[]):
        # Stores collected bindings
        bindings = []
        # If schema is dict (from CWL yaml)
        if isinstance(schema, dict):
            # For each CWL field
            for key, value in schema.items():
                # Checks if the current item has an inputBinding.
                if 'inputBinding' in value:
                    # Retrieves the inputBinding object.
                    binding = value['inputBinding']
                    # Constructs a position_key by appending the position if it is not None.
                    position_key = prefix_key + ([binding.position] if binding.position is not None else [])
                    # Appends a tuple containing the position_key, binding, and the corresponding value from input_obj to the bindings list.
                    bindings.append((position_key + [key], binding, input_obj.get(key)))

                # Checks if the current item is a dictionary and has a type field with a value of record, array, or map.
                if isinstance(value, dict) and 'type' in value and value['type'] in ['record', 'array', 'map']:
                    # Checks if the type is array and the corresponding value in input_obj is a list.
                    if value['type'] == 'array' and isinstance(input_obj.get(key), list):
                        # Iterates over the items in the list.
                        for i, item in enumerate(input_obj.get(key, [])):
                            # Recursively calls collect_bindings for each item in the list, updating the prefix_key.
                            bindings.extend(collect_bindings(value['items'], item, prefix_key + [binding.position, i] if binding.position is not None else [i]))
                    # Checks if the type is map and the corresponding value in input_obj is a dictionary.
                    elif value['type'] == 'map' and isinstance(input_obj.get(key), dict):
                        # Iterates over the items in the dictionary.
                        for map_key, map_value in input_obj.get(key, {}).items():
                            # Recursively calls collect_bindings for each item in the dictionary, updating the prefix_key.
                            bindings.extend(collect_bindings(value['items'], map_value, prefix_key + [binding.position, map_key] if binding.position is not None else [map_key]))
                    else:
                        # Recursively calls collect_bindings for other types, updating the prefix_key.
                        bindings.extend(collect_bindings(value, input_obj.get(key, {}), prefix_key))
        return bindings

    # Defines a nested function apply_binding to convert a binding to actual command-line elements.
    def apply_binding(binding: CommandLineBinding, value: Any) -> List[str]:
        # Initializes an empty list elements to store the command-line elements.
        elements = []
        if value is not None:
            # Checks if the binding has a prefix.
            if binding.prefix:
                # Appends the prefix and value as a string to the elements list.
                elements.append(binding.prefix + str(value))
            else:
                # If there is no prefix, appends the value as a string to the elements list.
                elements.append(str(value))
            if binding.separate:
                # Appends a space to the elements list.
                elements.append(' ')
        return elements

    # Collect bindings from arguments from CommandLineTool
    # Creates a list of tuples containing the position, index, binding, and None for each binding in tool.arguments.
    # NOTE: Why is None in tuple?
    argument_bindings = [([binding.position, i], binding, None) for i, binding in enumerate(tool.arguments)]

    # Collects bindings from the inputs schema of the CommandLineTool using the collect_bindings function.
    input_bindings = collect_bindings(tool.inputs, input_object)

    # Combine and sort bindings

    # Combines the argument_bindings and input_bindings into a single list all_bindings.
    all_bindings = argument_bindings + input_bindings
    # Sorts the all_bindings list based on the sorting key.
    all_bindings.sort(key=lambda x: (x[0], x[1].__class__.__name__ if x[1] else '', x[2]))

    # Build command line

    # Initializes an empty list command_line to store the command-line elements.
    command_line = []
    # Iterates over the sorted all_bindings list.
    for _, binding, value in all_bindings:
        # Extends the command_line list with the elements returned by apply_binding.
        command_line.extend(apply_binding(binding, value))

    # Insert baseCommand at the beginning of the command line
    command_line = tool.baseCommand + command_line

    return command_line

# Example usage
tool = CommandLineTool(
    baseCommand=["echo"],
    inputs={
        "message1": {
            "type": "string",
            "inputBinding": CommandLineBinding(position=2)
        },
        "message2": {
            "type": "string",
            "inputBinding": CommandLineBinding(position=1)
        },
        "message_list": {
            "type": ""
        }
    },
    arguments=[
        CommandLineBinding(position=3, prefix="--flag=")
    ]
)

input_object = {
    "message1": "input.txt",
    "message2": "arg1,arg2",
    "message_list": 
}

command_line = build_command_line(tool, input_object)
print(" ".join(command_line))
