"""
TODO TODO TODO TODO

NOTE Should workflow outputs return a delayed dict instead of dict 
FIXME process_images_sub.py executes subworkflow twice. Double compute call?
    Solution: See note above
    NOTE For now, removed @dask.delayed and .compute() calls from workflow 
         function definition.

CommandLineTool

Workflow
    TODO pickValue
    TODO linkMerge
    # TODO Step pickValue
    # TODO Step linkMerge

CommandLineTool AND Workflow
    # TODO Mutlityping
    TODO InitialWorkDirRequirement
    TODO InlineJavascriptRequirement: Include initial code if needed
    TODO Handle Enum complex input type
    TODO Multiline valueFrom
    TODO Arrays as default (step) input value
    TODO runtime context variables

TODO TODO TODO TODO
"""

import argparse, os

from pathlib import Path
from types import NoneType
from typing import (
    Any,
    Optional,
)
from uuid import uuid4

from cwl_utils.parser import (
    load_document_by_uri,
    CommandLineTool,
    ExpressionTool,    
    Process,
    Workflow,
)
from cwl_utils.parser.cwl_v1_2 import (
    CommandInputArraySchema,
    CommandInputParameter,
    CommandLineBinding,
    CommandOutputArraySchema, 
    CommandOutputBinding,
    CommandOutputParameter,
    Dirent,
    InputArraySchema,
    OutputArraySchema,
    WorkflowOutputParameter,
    WorkflowStep,
    WorkflowStepOutput,
)

# SDK Module name
SDK = "CWL2DASK.scripting"

# Whether to use the default Dask Client or jobqueue SLURM client
SLURM = False

# Whether code comments will be added to the script
COMMENTS = False


def tab(string: str, tab_amount: int = 1) -> str:
    """
    Apply `tab_amount` tabs to `string` and return it.
    """
    return "\t" * tab_amount + string

def comment(string: str) -> list[str]:
    """
    Wraps the string in a list if the transpiler has comments activated.
    Returns an empty list otherwise.
    """
    if COMMENTS:
        return [string]
    return []

def exists(o: object, key: str) -> bool:
    """ 
    Return whether `object` has a non-None valued attribute with name `key`.
    """
    return hasattr(o, key) and getattr(o, key) is not None

def is_expr(s: str) -> bool:
    """
    Return whether a string contains an expression.
    """
    if not isinstance(s, str):
        return False
    start = s.find("$(")
    end = s.find(")")
    return start > -1 and end > start


def normalize(string: str) -> str:
    """
    Normalize expressions by creating a single enclosed expression string.
    If the string does not contain expressions, the plain string is returned.
    Example:
        example_$(inputs.input).txt -> $('example_' + inputs.input + '.txt')
    Returns:
        The normalized expression string.
    """
    if not isinstance(string, str):
        raise TypeError("Should be string, but found", type(string))

    parts = []
    
    start = string.find("$(")
    if start < 0 or ")" not in string[start + 2:]:
        # String does not have an expression or not contain a
        # matching closing parenthesis
        return string

    for sub in string.split("$("):
        if sub != "" and sub.find(")") < 0:
            # not a matching ) in this substring
            parts.append(f"'{sub}'")

        # For each split
        pos = 0
        opened = 0
        for pos, char in enumerate(sub):
            # check characters for matching )
            if char == ")":
                if opened == 0:
                    split_point = pos
                    parts.append(sub[:split_point])
                    rest = sub[split_point + 1:]
                    if len(rest) > 0:
                        parts.append(f"'{rest}'")
                    break
                else:
                    opened -= 1
            elif char == "(":
                opened += 1    
    return "$(" + " + ".join(parts) + ")"


class ImportManager:
    imports: set
    from_imports: dict[str, set]

    def __init__(self):
        self.imports = set()
        self.from_imports = {}

        self.add("dask")
        self.add("subprocess")
        self.add("sys")
        self.add_from("dask.distributed", "Client")
        self.add_from(SDK, "load_input_object")

    def add(self, module):
        self.imports.add(module)

    def add_from(self, module, obj):
        if module in self.from_imports:
            self.from_imports[module].add(obj)
        else:
            self.from_imports[module] = set([obj])
    
    def get_lines(self) -> list[str]:
        # Generate and return the import statements
        ls = ["import " + ', '.join(sorted(self.imports))]
        ls.extend([f"from {k} import {', '.join(sorted(v))}" 
                    for k, v in sorted(self.from_imports.items())])
        ls.append("")
        return ls
    
IM = ImportManager()


"""
Mapping of CWL types to Python types. CWL supports types that base Python does not
recognize or support, like double and long. FIXME This is a band-aid for now.
"""
T_MAPPING: dict[str, str] = {
    "null": "NoneType",
    "boolean": "bool",
    "int": "int",
    "long": "int",
    "float": "float",
    "double": "float",
    "string": "str",
    "file": "FileObject",
    "directory": "DirectoryObject",
    "stdin": "FileObject",
    "stdout": "FileObject",
    "stderr": "FileObject",
}

class CWLType:
    is_array: bool
    optional: bool
    types: str | list[str]

    def __init__(self, type_):
        """
        """
        if isinstance(type_, (CommandInputArraySchema, InputArraySchema, CommandOutputArraySchema)):
            # TODO Optional arrays how?
            self.optional = False
            self.is_array = True
            type_ = type_.items
            self.types = T_MAPPING["".join([c.lower() for c in str(type_) if c not in ["?[]"]])]
        elif isinstance(type_, str):
            self.optional = "?" in type_
            self.is_array = "[]" in type_
            self.types = T_MAPPING["".join([c.lower() for c in type_ if c not in ["?[]"]])]
        elif isinstance(type_, list):
            type_ = type_.copy()
            # TODO Support multitypes
            # Union of types, can also be optional
            print(tab("Input binding has multiple types, which is not supported yet."))
            print(tab(f"Selecting the first non-null type found as input type instead."))
            self.optional = "null" in type_ or any(["?" in t for t in type_])

            if len(type_) > 1:
                type_.remove("null")
            type_ = type_[0]
            self.is_array = "[]" in type_
            self.types = T_MAPPING["".join([c.lower() for c in type_ if c not in ["?[]"]])]
        else:
            raise NotImplementedError(f"Found unsupported type {type(type_)}")


def convert_to_CWLType(value) -> CWLType:
    def convert_primitive(value):
        if isinstance(value, NoneType):
            t = "null"
        elif isinstance(value, bool):
            t = "boolean"
        elif isinstance(value, int):
            t = "int"
        elif isinstance(value, float):
            t = "float"
        elif isinstance(value, str):
            t = "string"
        elif isinstance(value, dict):
            if exists(value, "type"):
                if value.type in "File":
                    t = "file"
                elif value.type in "Directory":
                    t = "directory"
                else:
                    raise NotImplementedError("Dicts are not supported")
        return t

    if isinstance(value, list):
        if len(value) == 0:
            raise Exception("Empty list not supported")
        return CWLType(convert_primitive(value[0]) + "[]")
    return CWLType(convert_primitive(value))


def create_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="progname",
        description=""
    )

    parser.add_argument(
        "-i", "--input",
        required=True,
        type=str,
        help="CWL process that will be transpiled."
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        help="Filename of the output file containing the Python script."
    )
    parser.add_argument(
        "-s", "--slurm",
        action="store_true",
        help="Initialize the Dask client with a SLURM cluster for distributed workflow execution."
    )
    parser.add_argument(
        "-c", "--comments",
        action="store_true",
        help="Let the transpiler add descriptive comments to the code."
    )

    return parser


def gather_processes(
        path: Path, 
        processes: dict[str, Process], 
        ids: set,
        parent_step: Optional[WorkflowStep] = None,
        cache: bool = True,
    ) -> None:
    """
    Load the Process (and its subprocesses) from the file pointed to by `path`.
    If `cache` is `True`, the processes are cached in `processes`, indexed by
    the absolute path to the process file. Subprocesses with multiple 
    references are cached once.

    Additionally, adds the "subprocess" attribute to every `WorkflowStep`: A
    reference to the related Process object.
    """
    # Index by the absolute file path to prevent duplicates
    path = path.resolve()
    if path in processes:
        cache = False
    
    if cache:
        process = load_document_by_uri(path)
        process_id = process.id.split("#")[-1]
        if process_id in ids:
            raise Exception("Found duplicate process ID ", process_id)
        ids.add(process_id)
        processes[path] = process
    else:
        process = processes[path]

    if parent_step:
        setattr(parent_step, "subprocess", process)

    if isinstance(process, Workflow):
        for step in process.steps:
            step_path = Path(step.run[step.run.find(":") + 1:])
            if not step_path.is_absolute():
                step_path = path / step_path
            gather_processes(step_path, processes, ids, step, cache)
        return

    if not isinstance(process, CommandLineTool):
        raise TypeError(type(process), " is not a supported process type")
    

def parse_default(default, cwl_type: CWLType) -> str | list[str]:
    """
    TODO Add quotes to 'size', 'listing', 'contents' fields
    """
    FILE_KEYS = ["location", "path", "basename", "dirname", "nameroot",
                 "checksum", "size", "secondaryFiles", "contents"]
    DIR_KEYS = ["location", "path", "basename", "listing"]

    def parse_item(default):
        match cwl_type.types:
            case "bool" | "int" | "float": 
                value = default
            case "str":
                value = f'"{default}"'
            case "FileObject":
                IM.add_from(SDK, "FileObject")
                value = [f'"{k}":"{v}"' for k, v in default.items() if k in FILE_KEYS]
                value = f'FileObject({{{", ".join(value)}}})'
                
            case "DirectoryObject":
                IM.add_from(SDK, "DirectoryObject")
                value = [f'"{k}":"{v}"' for k, v in default.items() if k in DIR_KEYS]
                value = f'DirectoryObject({{{", ".join(value)}}})'
        return value

    if cwl_type.is_array:
        return [parse_item(d) for d in default]
    else:
        return parse_item(default)


def parse_process_input_parameter(input: CommandInputParameter) -> list[str]:
    """
    TODO Support complex default types, handle in CWLType class?
    """
    id = input.id.split("/")[-1]
    cwl_type = CWLType(input.type_)

    if exists(input, "default"):
        default = parse_default(input.default, cwl_type)
    else:
        if not exists(input, "inputBinding"):
            # Unbound inputs should not be listed
            return []
        # Non-optional inputs should crash the Process if they are missing.
        if cwl_type.optional:
            return [tab(f'"{id}": None,', 2)]
        else: 
            return []
    
    if cwl_type.is_array:
        return [
            tab(f'"{id}": [', 2),
            *[tab(f'{d},', 3) for d in default],
            tab("],", 2)
        ]
    else:
        return [tab(f'"{id}": {default},', 2)]
    
    # if isinstance(default, str):
    #     return [tab(f'"{id}": {default},', 2)]
    # else:
    #     return [
    #         tab(f'"{id}": [', 2),
    #         *[tab(f'{d},', 3) for d in default],
    #         tab("],", 2)
    #     ]


def parse_commandline(
        tool: CommandLineTool, 
        exprs: list[str]
    ) -> list[str]:
    """
    Generate a Python list that holds the commandline-building statements for 
    `tool`. Any expression handlers generated are added to `exprs`.

    NOTE: Only accept an integer as inputBinding.position value
    """
    n_func = 0
    def add_expression_function(expression: str, n_func: int) -> str:
        global IM
        IM.add_from(SDK, "js_eval")
        func_name = f"expr_handler_{n_func}"
        exprs.append(tab(f"def {func_name}(context: dict) -> str:"))
        exprs.append(tab(f'return js_eval("{expression}", context)', 2))
        return f"{func_name}(tool_context)"

    def compose_cmd_arg(
            value_or_expr: str,
            is_array: bool,
            binding: Optional[CommandLineBinding] = None,
            var_cast: bool = False,
        ) -> str:
        prefix = getattr(binding, "prefix", "")
        separate = getattr(binding, "separate", True)
        itemSeparator = getattr(binding, "itemSeparator", None)
        if is_array:
            if itemSeparator:
                if prefix and separate:         # -i= A,B,C
                    arg = f'"{prefix}", '
                    arg += f'{itemSeparator}.join(str(x) for x in {value_or_expr})'
                elif prefix and not separate:   # -i=A,B,C
                    arg = f'"{prefix}"'
                    arg += f'{itemSeparator}.join(str(x) for x in {value_or_expr})'
                else:                           # A,B,C
                    arg = f'{itemSeparator}.join(str(x) for x in {value_or_expr})' 
            else:
                if prefix and separate:         # -i= A B C
                    arg = f'"{prefix}", '
                    arg += f'*[str(v) for v in {value_or_expr}]'
                if prefix and not separate:     # -i=A -i=B -i=C
                    arg = f'*["{prefix}" + str(v) for v in {value_or_expr}]'
                else:                           # A B C
                    arg = f'*[str(v) for v in {value_or_expr}]'
        else:
            if prefix:
                if separate:
                    if var_cast:
                        arg = f'"{prefix}", str({value_or_expr})'
                    else:
                        arg = f'"{prefix}", {value_or_expr}'
                else:
                    arg = f'"{prefix}" + {value_or_expr}'
            else:
                arg = f'str({value_or_expr})' if var_cast else value_or_expr
        return arg

    # Each tuple stores:
    # (position, argument index, value expression, is-array, binding object, var_cast)
    ordered_items: list[tuple[int, int, str, bool, object | None, bool]] = []

    # Assign a sorting key (inputBinding.position, argument index) to the tool
    # arguments.
    if exists(tool, "arguments"):
        for i, arg in enumerate(tool.arguments):
            var_cast = False
            if isinstance(arg, str):
                arg = normalize(arg)
                if is_expr(arg):
                    var_cast = True
                    arg = add_expression_function(arg[2:-1], n_func)
                    n_func += 1
                else:
                    arg = f'"{arg}"'
                ordered_items.append((0, i, arg, False, None, var_cast))
            elif isinstance(arg, CommandLineBinding):
                value_or_expr = normalize(arg.valueFrom)
                if is_expr(value_or_expr):
                    var_cast = True
                    value_or_expr = add_expression_function(value_or_expr[2:-1], n_func)
                    n_func += 1
                else:
                    value_or_expr = f'"{value_or_expr}"'

                # Read the position attribute. If 'None' is found, set default
                pos = getattr(arg, "position", 0)
                if pos is None: 
                    pos = 0
                ordered_items.append((pos, i, value_or_expr, False, arg, var_cast))
            else:
                raise TypeError(f"Unsupported argument type: {type(arg)}")
 
    # Assign a sorting key (inputBinding.position, argument index) to the tool
    # inputs.
    for input_ in tool.inputs:        
        if not exists(input_, "inputBinding"):  # Unbound input
            continue

        input_id = input_.id.split("/")[-1]
        binding = input_.inputBinding
        t = CWLType(input_.type_)
        pos: int = getattr(binding, "position", 0)
        value_or_expr = f'inputs["{input_id}"]'
        var_cast = True

        # If the binding has valueFrom, add a expression handler if needed
        if exists(binding, "valueFrom"):
            value_or_expr = binding.valueFrom
            if is_expr(value_or_expr):
                value_or_expr = normalize(value_or_expr)
                value_or_expr = add_expression_function(value_or_expr[2:-1])
            else:
                var_cast = False
                value_or_expr = f'"{value_or_expr}"'
        ordered_items.append((pos, len(ordered_items), value_or_expr, t.is_array, binding, var_cast))

    # Both the inputs with an inputBinding as well as the tool arguments are
    # sorted, prefixed with the baseCommand to produce the final command.
    command_items: list[str] = []
    if exists(tool, "baseCommand"):
        baseCommand = tool.baseCommand
        if isinstance(baseCommand, str):
            command_items.append(f"'{baseCommand}'")
        elif isinstance(baseCommand, list):
            command_items.extend([f"'{s}'"  for s in baseCommand])
        else:
            raise TypeError(f"Unsupported baseCommand type: {type(baseCommand)}")
    
    # Sort and apply the commandline bindings from arguments/inputs
    ordered_items.sort(key=lambda item: (item[0], item[1]))
    for _, _, value_or_expr, is_array, binding, var_cast in ordered_items:
        command_items.append(compose_cmd_arg(value_or_expr, is_array, binding, var_cast))

    if len(command_items) > 1:
        lines = [tab("cmd = [")]
        for item in command_items:
            lines.append(tab(f"{item},", 2))
        lines.append(tab("]"))
    else:
        lines = [tab(f'cmd = [{command_items[0]}]')]
    return lines


def parse_run(
        tool: CommandLineTool,
        requirements,
        exprs: list[str]
    ) -> list[str]:
    global IM
    lines = []
    run_lines = []
    clean_up = []

    def uses_stdout() -> None | str:
        """
        Returns the stdout filename or stdout expression handler function as a
        string.
        Adds/overwrites stdout in the respective output's outputBinding.glob.
        """
        stdout = None
        random_stdout = f"stdout_{str(uuid4())}"
        if exists(tool, "stdout"):
            random_stdout = tool.stdout
            stdout = tool.stdout
        
        for output in tool.outputs:
            # type_ may contain complex type, which can be ignored
            if not isinstance(output.type_, str): 
                continue

            if "stdout" in output.type_:
                if exists(output, "outputBinding"):
                    if exists(output.outputBinding, "glob"):
                        stdout = output.outputBinding.glob
                    else:
                        setattr(output.outputBinding, "glob", random_stdout)
                        stdout = random_stdout
                else:
                    setattr(output, "outputBinding", CommandOutputBinding(glob=random_stdout))
                    stdout = random_stdout
                break
        return stdout

    def envVar_handler(var) -> str:
        envValue = var.envValue
        if is_expr(envValue):
            envValue = normalize(envValue)
            IM.add_from(SDK, "js_eval")
            exprs.append(tab(f'def env_{var.envName}(context):'))
            exprs.append(tab(f'return js_eval({envValue[2:-1]}, context)', 2))
            return f'env_{var.envName}(tool_context)'
        return f'"{envValue}"'

    # Parse stdin, stdout, stderr
    if exists(tool, "stdin"):
        if is_expr(tool.stdin):
            stdin = normalize(tool.stdin)
            IM.add_from(SDK, "js_eval")
            exprs.append(tab("def stdin_handler(context):"))
            exprs.append(tab(f'return js_eval("{stdin[2:-1]}", context)', 2))
            stdin = "stdin_handler(tool_context)"
        else:
            stdin = f'"{stdin}"'

        lines.append(tab(f'stdin = open({stdin}, "r")'))
        run_lines.append("stdin=stdin")
        clean_up.append(tab(f'stdin.close()'))

    stdout = uses_stdout()
    if stdout:
        if is_expr(stdout):
            stdout = normalize(stdout)
            IM.add_from(SDK, "js_eval")
            exprs.append(tab("def stdout_handler(context):"))
            exprs.append(tab(f'return js_eval("{stdout[2:-1]}", context)', 2))
            stdout = "stdout_handler(tool_context)"
        else:
            stdout = f'"{stdout}"'

        lines.append(tab(f'stdout = open({stdout}, "w")'))
        run_lines.append("stdout=stdout")
        clean_up.append(tab(f'stdout.close()'))

    if exists(tool, "stderr"):
        if is_expr(tool.stderr):
            stderr = normalize(tool.stderr)
            IM.add_from(SDK, "js_eval")
            exprs.append(tab("def stderr_handler(context):"))
            exprs.append(tab(f'return js_eval("{stderr[2:-1]}", context)', 2))
            stderr = "stderr_handler(tool_context)"
        else:
            stderr = f'"{stderr}"'

        lines.append(tab(f'stderr = open({stderr}, "w")'))
        run_lines.append("stderr=stderr")
        clean_up.append(tab(f'stderr.close()'))

    # Parse EnvVarRequirement
    if "EnvVarRequirement" in requirements:
        if len(requirements["EnvVarRequirement"].envDef) > 1:
            lines.append(tab("env = {"))
            for var in requirements["EnvVarRequirement"].envDef:
                envValue = envVar_handler(var)
                lines.append(tab(f'"{var.envName}": {envValue},', 2))
            lines.append(tab("}"))
        else:
            var = requirements["EnvVarRequirement"].envDef[0]
            envValue = envVar_handler(var)
            lines.append(tab(f'env = {{"{var.envName}": {envValue}}}'))
        run_lines.append("env=env")

    # If there are optional command-line arguments, we need to filter for None
    if any([CWLType(i.type_).optional for i in tool.inputs]):
        lines.append(tab('cmd = [x for x in cmd if x]'))

    lines.append(tab('print("Running:",  *cmd)'))
    if len(run_lines) == 0:
        return lines + [tab("subprocess.run(cmd)")] + clean_up
    else:
        return lines + [
            tab("subprocess.run("),
            tab("args=cmd,", 2),
            *[tab(f'{l},', 2) for l in run_lines],
            tab(")"),
        ] + clean_up


def parse_tool_output_binding(
        output: CommandOutputParameter, 
        exprs: list[str]
    ) -> str:
    """
    Return an output assignment for a CWL output.
    """
    global IM
    id = output.id.split("/")[-1]
    t = CWLType(output.type_)

    if "FileObject" in t.types:
        IM.add_from(SDK, "FileObject")
    if "DirectoryObject" in t.types:
        IM.add_from(SDK, "DirectoryObject")

    # Create expression handler that handles an output's glob matching
    # and outputEval.
    binding = output.outputBinding
    exprs.append(tab(f"def outputs_{id}(context):"))
    glob_flag = False
    x = ""
    if exists(binding, "glob"):
        glob_flag = True
        g = binding.glob
        IM.add_from(SDK, "glob")
        if isinstance(g, str):
            if is_expr(g):
                g = normalize(g)
                # Expression
                IM.add_from(SDK, "js_eval")
                exprs.append(tab(f'pattern = js_eval("{g[2:-1]}", context)', 2))
                x = "glob(pattern)"
            else:
                # Simple string
                x = f'glob("{g}")'
        else:
            # List of simple strings
            patterns = ", ".join([f'"{p}"' for p in g])
            exprs.append(tab(f'pattern = [{patterns}]'), 2)
            x = "glob(pattern)"

    if exists(binding, "outputEval"):
        IM.add_from(SDK, "js_eval")
        if glob_flag:
            IM.add_from(SDK, "FileObject")
            exprs.append(tab(f'matches = {x}', 2))
            loadContents = ""
            if exists(binding, "loadContents"):
                loadContents = ", loadContents = True"
            exprs.append(tab(f'context["self"] = [FileObject(m{loadContents}) for m in matches]', 2))
        exprs.append(tab(f'return js_eval("{binding.outputEval[2:-1]}", context)', 2))
    else:
        p = "" if t.is_array else "[0]"
        exprs.append(tab(f"return {t.types}({x}{p})", 2))

    return tab(f'"{id}": outputs_{id}(tool_context),', 2)


def parse_tool(tool: CommandLineTool) -> list[str]:
    header:  list[str] = []
    exprs:   list[str] = []
    inputs:  list[str] = []
    command: list[str] = []
    outputs: list[str] = []

    requirements: dict[str, Any] = {}
    if exists(tool, "requirements"):
        requirements = {str(req.class_): req for req in tool.requirements}

    # header
    tool_id = tool.id.split("#")[-1]
    if "file://" in tool_id:
        raise Exception(f"CWL file {tool_id} misses an ID, which is required!")
    
    header.append('@dask.delayed')
    header.append(f'def {tool_id}(input_obj: dict, context: dict) -> dict:')
    
    # Metadata
    header.append(tab('"""'))
    header.append(tab('class: CommandLineTool'))
    if exists(tool, "label"):
        header.append(tab('label: ' + tool.label))
    header.append(tab('"""'))

    # Input object to inputs
    inputs.extend(comment(tab("# Gather inputs in their correct format")))
    # Parse default values or None for optionals
    input_params = []
    for i in tool.inputs:
        input_params.extend(parse_process_input_parameter(i))
    if len(input_params) > 0:
        inputs.append(tab("inputs = {"))
        inputs.extend(input_params)
        inputs.append(tab("}"))
    else:
        inputs.append(tab("inputs = {}"))

    inputs.append(tab("inputs.update(input_obj)"))
    inputs.append(tab('tool_context = {"inputs": inputs} | context'))
    context_pos = len(inputs)
    inputs.append("")

    # Parse command
    command.extend(comment(tab("# Ready the commandline and execute the tool")))
    command.extend(parse_commandline(tool, exprs))
    command.extend(parse_run(tool, requirements, exprs))
    command.append("")

    # Parse outputs
    outputs.extend(comment(tab("# Collect and generate outputs")))
    outputs.append(tab("return {"))
    for o in tool.outputs:
        outputs.append(parse_tool_output_binding(o, exprs))
    outputs.append(tab("}"))

    # Remove tool_context statement if no expressions are used
    if len(exprs) == 0:
        inputs.pop(context_pos - 1)
    exprs.append("")

    return header + exprs + inputs + command + outputs


def parse_workflow_step_inputs(
        step: WorkflowStep, 
        step_id: str
    ) -> list[str]:
    """
    
    """

    def extract_source(source: str):
        keys = source.split("#")[-1].split("/")
        if len(keys) == 2:
            # Source is a workflow input: process_id/input_id
            return f'inputs["{keys[1]}"]'
        else: # Source is other step input: process_id/step_id/input_id
            return f'{keys[1]}_out["{keys[2]}"]'

    global IM
    lines: list[str] = [tab(f'{step_id}_in = {{')]
    for input in step.in_:
        input_id = input.id.split("/")[-1]

        if exists(input, "default"):
            default = parse_default(input.default, convert_to_CWLType(input.default))
        
        use_linkMerge = False
        use_pickValue = False
        linkMerge = "merge_nested"
        if exists(input, "linkMerge"):
            if input.linkMerge not in ("merge_nested", "merge_flattened"):
                raise ValueError("Expected 'merge_nested' or merge_flattened, but got", input.linkMerge)
            use_linkMerge = True
            linkMerge = input.linkMerge
            IM.add_from(SDK, linkMerge)
        if exists(input, "pickValue"):
            if input.pickValue not in ('first_non_null', 'the_first_non_null', 'all_non_null'):
                raise ValueError("Expected 'first_non_null', 'the_first_non_null', or 'all_non_null', but got", input.pickValue)
            use_pickValue = True
            pickValue = input.pickValue
            IM.add_from(SDK, pickValue)

        if exists(input, "source"):
            source = input.source
            if isinstance(source, list):
                n_sources = len(source)
                if n_sources == 1:
                    # Single source
                    source = extract_source(source[0])
                    if use_linkMerge:
                        source = f'[{source}]'
                elif n_sources > 1:
                    # Multiple sources
                    sources = [extract_source(s) for s in source]
                    source = ", ".join(sources)
                    if use_linkMerge:
                        source = f'{linkMerge}({source})'
                    if use_pickValue:
                        source = f'{pickValue}({source})'
            else:
                # Single source
                source = extract_source(source)                        

        if exists(input, "default") and exists(input, "source"):
            # Default+source: Add if statement that selects right input
            if isinstance(default, str):
                lines.append(tab(f'"{input_id}": {source} if {source} else {default}', 2))
            else:
                lines.extend([
                    tab(f'"{input_id}": {source} if {source} else [', 2),
                    *[tab(f'{d},', 3) for d in default],
                    tab("],", 2)
                ])
        elif exists(input, "default"):
            if isinstance(default, str):
                lines.append(tab(f'"{input_id}": {default},', 2))
            else:
                lines.extend([
                    tab(f'"{input_id}": [', 2),
                    *[tab(f'{d},', 3) for d in default],
                    tab("],", 2)
                ])
        elif exists(input, "source"):
            # Source
            lines.append(tab(f'"{input_id}": {source},', 2))
    lines.append(tab('}'))
    return lines


def parse_workflow_step(step: WorkflowStep, exprs: list[str]) -> list[str]:
    """
    TODO
    """
    global IM
    step_id = step.id.split("/")[-1]

    def parse_valueFrom(scattered: bool, tabs: int = 1) -> list[str]:
        input_dict = "scattered_inputs" if scattered else step_id + "_in"


        lines: list[str] = []
        for input in step.in_:
            if exists(input, "valueFrom"):
                valueFrom = input.valueFrom
                input_id = input.id.split("/")[-1]
                if is_expr(input.valueFrom):
                    expr = normalize(input.valueFrom)[2:-1]
                    IM.add_from(SDK, "js_eval")
                    if exists(input, "source") or exists(input, "default"):
                        # exprs.append(tab(f"def {step_id}_{input_id}(context, self):"))
                        exprs.append(tab(f"def {step_id}_{input_id}(context):"))
                        # exprs.append(tab('context["self"] = self', 2))
                        lines.append(tab(f'{input_dict}["{input_id}"] = {step_id}_{input_id}(tool_context | {{"self": {step_id}_in["{input_id}"]}})', tabs))
                    else:
                        exprs.append(tab(f"def {step_id}_{input_id}(context):"))
                        # exprs.append(tab('context["self"] = None', 2))
                        lines.append(tab(f'{input_dict}["{input_id}"] = {step_id}_{input_id}(tool_context)', tabs))
                    exprs.append(tab(f'return js_eval("{expr}", context)', 2))
                        
                else:
                    lines.append(tab(f'{input_dict}["{input_id}"] = "{valueFrom}"', tabs))
        return lines
    
    # Parse step metadata
    lines = []
    lines.append(tab(f'# Step ID:    {step_id}'))
    if exists(step, "label"):
        lines.append(tab(f'# Step label: {step.label}'))

    # Parse step inputs (source/default)
    lines.extend(parse_workflow_step_inputs(step, step_id))

    # Parse when
    x = 0   # Extra tabs to be inserted resulting from the when conditional
    if exists(step, "when"):
        x = 1
        IM.add_from(SDK, "js_eval")
        when = normalize(step.when)[2:-1]
        exprs.append(tab(f'def {step_id}_when(context):'))
        exprs.append(tab(f'return js_eval("{when}", context)', 2))
        lines.append(tab(f'tool_context["inputs"] = {step_id}_in'))
        lines.append(tab(f'if {step_id}_when(tool_context):'))

    # Parse step context and execution
    subprocess_id = step.subprocess.id.split("#")[-1]
    use_valueFrom = any(exists(i, "valueFrom") for i in step.in_)
    if exists(step, "scatter"):
        IM.add_from(SDK, "scatterizer")
        IM.add_from(SDK, "transpose")
        lines.append(tab(f'{step_id}_scattered_out = []', 1 + x))
        lines.append(tab(f'for scattered_inputs in scatterizer({step_id}_in, "input"):', 1 + x))
        lines.append(tab(f'tool_context["inputs"] = inputs | scattered_inputs', 2 + x))
        lines.extend(parse_valueFrom(True, 2 + x))
        lines.append(tab(f'{step_id}_scattered_out.append({subprocess_id}(scattered_inputs, context))', 2 + x))
        lines.append(tab(f"{step_id}_out = dask.delayed(transpose)({step_id}_scattered_out)", 1 + x))
    else:
        if use_valueFrom:
            lines.append(tab(f'tool_context["inputs"] = inputs | {step_id}_in'))
        lines.extend(parse_valueFrom(False, 1 + x))
        lines.append(tab(f'{step_id}_out = {subprocess_id}({step_id}_in, context)', 1 + x))

    # Parse when null results
    if x == 1:
        lines.append(tab("else:"))
        lines.append(tab(f'{step_id}_out = {{', 2))
        for out in step.out:
            out_id = out if isinstance(out, str) else out.id
            lines.append(tab(f'"{out_id.split("/")[-1]}": None,', 3))
        lines.append(tab("}", 2))

    lines.append("")
    return lines


def parse_workflow_output(output: WorkflowOutputParameter) -> str:
    """
    # TODO
    """
    outputSource = output.outputSource
    if len(outputSource) > 1:
        linkMerge = "merge_nested"
        if exists(output, "linkMerge"):
            linkMerge = output.linkMerge
            # TODO
        # TODO
           
        raise NotImplementedError("Output multisourcing is currently not supported")
    
    step_id, input_id = output.outputSource[0].split("/")[-2:]
    output_id = output.id.split("/")[-1]
    return tab(f'"{output_id}": {step_id}_out["{input_id}"],', 2)
    # return tab(f'"{output_id}": {step_id}_out["{input_id}"].compute(),', 2)


def parse_workflow(wf: Workflow):
    """
    TODO
    """
    header:  list[str] = []
    exprs:   list[str] = []
    inputs:  list[str] = []
    steps:   list[str] = []
    outputs: list[str] = []

    # header
    wf_id = wf.id.split("#")[-1]
    # header.append('@dask.delayed')
    header.append(f'def {wf_id}(input_obj: dict, context: dict) -> dict:')
    
    # Metadata
    header.append(tab('"""'))
    header.append(tab('class: Workflow'))
    if exists(wf, "label"):
        header.append(tab('label: ' + wf.label))
    header.append(tab('"""'))

    # Input object to inputs
    inputs.extend(comment(tab("# Gather inputs in their correct format")))
    # Parse default/optional values
    input_params = []
    for i in wf.inputs:
        input_params.extend(parse_process_input_parameter(i))
    if len(input_params) > 0:
        inputs.append(tab("inputs = {"))
        inputs.extend(input_params)
        inputs.append(tab("}"))
    else:
        inputs.append(tab("inputs = {}"))


    inputs.append(tab("inputs.update(input_obj)"))
    inputs.append(tab('tool_context = {"inputs": inputs} | context'))
    # context_pos = len(inputs)
    inputs.append("")

    # Parse steps
    for step in wf.steps:
        steps.extend(parse_workflow_step(step, exprs))

    # Parse outputs
    outputs.extend(comment(tab("# Compute outputs")))
    outputs.append(tab("return {"))
    for output in wf.outputs:
        outputs.append(parse_workflow_output(output))
    outputs.append(tab("}"))


    # # Remove tool_context statement if no expressions are used
    # if len(exprs) == 0:
    #     inputs.pop(context_pos - 1)
    if len(exprs) > 0:
        exprs.append("")
        
    return header + exprs + inputs + steps  + outputs


def parse_main(main_id: str) -> list[str]:
    """
    Create the script main entry.
    """
    ls: list[str] = ["def main():"]

    # Write DASK client initialization
    ls.extend(comment(tab("# Initialize cluster")))
    if SLURM:
        ls.extend(comment(tab("# NOTE: Memory argument is forced by the SLURMCluster ")))
        ls.extend(comment(tab("# initializer. This causes problems on systems that disable")))
        ls.extend(comment(tab("# setting memory requirements (DAS6 has this restriction). The")))
        ls.extend(comment(tab("# band-aid is to ignore the memory setting line with")))
        ls.extend(comment(tab("# 'job_directives_skip'.")))
        ls.append(tab('cluster = SLURMCluster('))
        ls.append(tab('cores=16,', 2))
        ls.append(tab('memory="16GB",', 2))
        ls.append(tab('walltime="00:15:00",', 2))
        ls.append(tab('job_directives_skip=[\'--mem\']', 2))
        ls.append(tab(")"))
        ls.append(tab("cluster.scale(4)"))
        ls.append(tab("client = Client(cluster)", 1))
    else:
        ls.append(tab("client = Client()"))

    ls.append("")
    ls.extend(comment(tab("# Convert input YAML to dict")))
    ls.append(tab('input_obj = load_input_object(sys.argv[1])'))
    ls.append("")
    ls.extend(comment(tab("# Initialize CWL context")))
    ls.append(tab("context = {}"))
    ls.append("")
    ls.extend(comment(tab("# Submit to DASK")))
    ls.append(tab(f"result = client.compute({main_id}(input_obj, context)).result()"))
    ls.append(tab("print(*[f'{k}: {v}' for k, v in result.items()])"))
    ls.append("")
    ls.append('if __name__ == "__main__":')
    ls.append(tab("main()"))
    return ls


def parse_cwl(cwl_path):
    global IM
    body_lines: list[str] = []
    processes: dict[str, Process] = {}
    ids = set()

    # Gather all unique procesess in preorder fashion
    gather_processes(cwl_path, processes, ids)

    # Parse tools and workflow functions inorder
    for process in reversed(processes.values()):
        if isinstance(process, ExpressionTool):
            raise NotImplementedError("ExpressionTool transpilation is not supported")
        if isinstance(process, CommandLineTool):
            body_lines.extend(parse_tool(process))
        elif isinstance(process, Workflow):
            body_lines.extend(parse_workflow(process))
        else:
            raise TypeError("Unsupported CWL Process type", type(process))
        body_lines.append("")
        body_lines.append("")

    main_id = processes[cwl_path].id.split("#")[-1]
    return IM.get_lines() + body_lines + parse_main(main_id)


def main():
    arg_parser = create_arg_parser()
    args = arg_parser.parse_args()

    cwl_path = Path(args.input).resolve()
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(cwl_path.stem + ".py")
    global SLURM, COMMENTS, IM

    if args.slurm:
        IM.add_from("dask_jobqueue.slurm", "SLURMCluster")
        SLURM = True
    if args.comments:
        COMMENTS = True 

    # # Load CWL process into an object
    # cwl = load_document_by_uri(cwl_path)

    # # Expression tools are extracted as normal tools
    # if isinstance(cwl, ExpressionTool):
    #     raise NotImplementedError("ExpressionTool transpilation is not supported")
    print("Transpiling", str(cwl_path), "to", str(output_path), "...")
    lines = parse_cwl(cwl_path)
    with open(output_path, "w") as output_file:
        output_file.writelines([f'{l}\n' for l in lines])


if __name__ == "__main__":
    main()
