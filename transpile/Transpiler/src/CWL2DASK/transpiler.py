"""
TODO TODO TODO TODO

BUG: load_document_by_url fails when InlineJavascriptRequirement.expressionLib
     holds an include statement -> {$include: somefile.js}. This happens
     because CWL expects an engine to replace $include statements during
     preprocessing. 

CommandLineTool

Workflow

CommandLineTool AND Workflow
    TODO work directory
    TODO InitialWorkDirRequirement
    TODO? Mutlityping
    TODO? ResourceRequirement

TODO TODO TODO TODO
"""

import argparse, os

from pathlib import Path
from types import NoneType
from typing import (
    Any,
    Optional,
    Mapping,
)
from uuid import uuid4

from cwl_utils.parser import (
    load_document_by_uri,
    CommandLineTool,
    Directory as CWLDirectory,
    ExpressionTool,
    File as CWLFile,    
    Process,
    Workflow,
)
from cwl_utils.parser.cwl_v1_2 import (
    CommandInputArraySchema,
    CommandInputEnumSchema,
    CommandInputParameter,
    CommandLineBinding,
    CommandOutputArraySchema, 
    CommandOutputBinding,
    CommandOutputEnumSchema, 
    CommandOutputParameter,
    Dirent,
    InputArraySchema,
    OutputArraySchema,
    InputEnumSchema,
    WorkflowOutputParameter,
    WorkflowStep,
    WorkflowStepOutput,
)

# SDK Module name
SDK = "CWL2DASK.scripting"
from CWL2DASK.scripting import FileObject, DirectoryObject

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
    Normalize expressions by creating a single enclosed expression string. Leading and trailing whitespaces are removed. 
    If the string does not contain expressions, the stripped string is returned.
    Example:
        example_$(inputs.input).txt -> $('example_' + inputs.input + '.txt')
    Returns:
        The normalized expression string.
    """
    if not isinstance(string, str):
        raise TypeError("Should be string, but found", type(string))

    parts = []
    string = string.strip()
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


def multiline_to_list(string: str, normalize_js_expr: bool = True) -> list[str]:
    """
    Convert multi-line YAML strings (indicated by '- |') to a list of normal
    strings.
    """
    # # YAML uses some character escaping that doesnt work in python strings.
    # # They must be removed from the literal string.
    # lines = bytes(string.replace(r"\$", "$"), "utf-8").decode("unicode_escape")

    if not isinstance(string, str):
        raise TypeError("Expected 'str', but got", type(string))

    # Because we need to encase strings in double quotation marks, we need to
    # add escapes.
    lines = string.strip().replace('"', r'\"').split("\n")
    if normalize_js_expr:
        ret = []
        for l in lines:
            if is_expr(l):
                ret.append(normalize(l)[2:-1])
            else:
                ret.append(l)
        return ret
    else:
        return lines

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
        def filter(string, chars):
            return "".join([c.lower() for c in str(string) if c not in chars])
        
        if isinstance(type_, (CommandInputArraySchema, 
                              InputArraySchema, 
                              CommandOutputArraySchema)):
            # TODO Optional arrays how?
            self.optional = False
            self.is_array = True
            type_ = type_.items
            self.types = T_MAPPING[filter(type_, "?[]")]
        elif isinstance(type_, str):
            self.optional = "?" in type_
            self.is_array = "[]" in type_
            self.types = T_MAPPING[filter(type_, "?[]")]
        elif (isinstance(type_, list) and
              isinstance(type_[0], (CommandInputEnumSchema,
                                    CommandOutputEnumSchema,
                                    InputEnumSchema))):
                # Enums are actually just special strings
                # TODO Optional enum how?
                self.optional = False
                self.is_array = False
                self.types = "str"
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
            self.types = T_MAPPING[filter(type_, "?[]")]
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
        elif isinstance(value, CWLFile):
            t = "file"
        elif isinstance(value, CWLDirectory):
            t = "directory"
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

    # BUG 
    $include statement in `expressionLib` causes exception in `load_document_by_uri`\n
    TODO Somehow parse/replace ($include: ...) statements.
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
                if isinstance(default, CWLFile):
                    value = [f'"{a}":"{getattr(default, a)}"' 
                             for a in FILE_KEYS 
                             if getattr(default, a) != "" and 
                                getattr(default, a) is not None]
                elif isinstance(default, Mapping):
                    value = [f'"{k}":"{v}"' for k, v in default.items() 
                                            if k in FILE_KEYS]
                value = f'FileObject({{{", ".join(value)}}})'
                
            case "DirectoryObject":
                IM.add_from(SDK, "DirectoryObject")
                if isinstance(default, CWLDirectory):
                    value = [f'"{a}":"{getattr(default, a)}"' 
                             for a in DIR_KEYS 
                             if getattr(default, a) != "" and 
                                getattr(default, a) is not None]
                elif isinstance(default, Mapping):
                    value = [f'"{k}":"{v}"' for k, v in default.items() 
                                            if k in DIR_KEYS]
                value = f'DirectoryObject({{{", ".join(value)}}})'
        return value

    if cwl_type.is_array:
        return [parse_item(d) for d in default]
    else:
        return parse_item(default)


def parse_init_work_dir_req(
        req,
        exprs: list[str],
        js: list[str]
    ) -> list[str]:
    global IM
    IM.add_from(SDK, "initial_work_dir_requirement")

    num_exprs = 0
    def add_expr(expr: list[str], num_exprs) -> str:
        IM.add_from(SDK, "js_eval")
        exprs.append(tab(f"def stage_expr_{num_exprs}(context):"))
        if len(expr) == 1:
            exprs.append(tab(f'return js_eval({expr[0]}, context{js})', 2))
        else:
            exprs.append(tab("expr = [", 2))
            for line in expr:
                exprs.append(tab(f'"{line}"', 3))
            exprs.append(tab("]", 2))
            exprs.append(tab(f'return js_eval(expr, context{js})', 2))
        func = f"stage_expr_{num_exprs}(tool_context)"
        return func

    lines: list[str] = []
    listing = req.listing
    if isinstance(listing, str):
        # File/Dir(s) come from expression
        lines.append(tab(f'initial_work_dir_requirement({add_expr(listing, num_exprs)}'))
    else:
        lines.append(tab(f'initial_work_dir_requirement(['))
        for e in listing:
            if isinstance(e, str):
                # File/Dir(s) come from expression
                lines.append(tab(f'{add_expr(e, num_exprs)},', 2))
            elif isinstance(e, Dirent):
                lines.append(tab('{', 2))
                if exists(e, "entryname"):
                    if is_expr(e.entryname):
                        expr = normalize(e.entryname)[2:-1]
                        expr = [f'"{expr}"']
                        lines.append(tab(f'"entryname": {add_expr(expr, num_exprs)},', 3))
                        num_exprs += 1
                    else:
                        lines.append(tab(f'"entryname": "{e.entryname}",', 3))
                if exists(e, "entry"):
                    # Multiline entry scripts are put into lists of
                    # lines to keep it simple.
                    entry_lines = multiline_to_list(e.entry, False)
                    if len(entry_lines) == 1:
                        if is_expr(entry_lines[0]):
                            expr = [f'"{entry_lines[0][2:-1]}"']
                            lines.append(tab(f'"entry": {add_expr(expr, num_exprs)},', 3))
                            num_exprs += 1
                        else:
                            lines.append(tab(f'"entry": "{entry_lines[0]}",', 3))
                    else:
                        lines.append(tab('"entry": [', 3))
                        for l in entry_lines:
                            if is_expr(l):
                                l = [f'"{normalize(l)[2:-1]}"']
                                lines.append(tab(f'{add_expr(l, num_exprs)},', 4))
                                num_exprs += 1
                            else:
                                lines.append(tab(f'"{l}",', 4))
                        lines.append(tab('],', 3))
                if exists(e, "writable"):
                    lines.append(tab(f'"writable": "{e.writable}",', 3))
                lines.append(tab('},', 2))
            elif isinstance(e, CWLFile):
                IM.add_from(SDK, "FileObject")
                file_dict = e.save()
                lines.append(tab("FileObject({", 2))
                for k, v in file_dict.items():
                    lines.append(tab(f'"{k}": "{v}",', 3))
                lines.append(tab(f'"{k}": "{v}",', 3))
                lines.append(tab("}),", 2))
            elif isinstance(e, CWLDirectory):
                IM.add_from(SDK, "DirectoryObject")
                dir_dict = e.save()
                lines.append(tab("DirectoryObject({", 2))
                for k, v in dir_dict.items():
                    lines.append(tab(f'"{k}": "{v}",', 3))
                lines.append(tab(f'"{k}": "{v}",', 3))
                lines.append(tab("}),", 2))
        lines.append(tab("])"))
    return lines  + [""]


def parse_js_req(expressionLib):
        """
        Parse InlineJavascriptRequirement.expressionLib and return all lines of
        Javascript code as a list of strings.

        # BUG 
        $include statement in `expressionLib` causes exception in `load_document_by_uri`\n
        TODO Somehow parse/replace ($include: ...) statements.
        """
        concat: str = ""
        for expr in expressionLib:
            if concat == "":
                concat = expr
            else:
                concat += "\n" + expr

        lines = []
        js_context = multiline_to_list(concat)
        if len(js_context) == 1:
            lines.append(tab(f'js_context = ["{js_context[0]}"]\n'))
        elif len(js_context) > 1:
            lines.append(tab("js_context = ["))
            for l in js_context:
                lines.append(tab(f'"{l}",', 2))
            lines.append(tab("]\n"))
        return lines


def parse_process_input_parameter(input: CommandInputParameter) -> list[str]:
    """
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


def parse_commandline(
        tool: CommandLineTool, 
        exprs: list[str],
        js: str
    ) -> list[str]:
    """
    Generate a Python list that holds the commandline-building statements for 
    `tool`. Any expression handlers generated are added to `exprs`.

    NOTE: Only accept an integer as inputBinding.position value
    """
    n_func = 0
    def add_expr_function( expr: list[str], n_func: int) -> str:
        global IM
        IM.add_from(SDK, "js_eval")
        func_name = f"expr_handler_{n_func}"
        exprs.append(tab(f"def {func_name}(context: dict) -> str:"))
        if len(expr) == 1:
            exprs.append(tab(f'return js_eval("{expr[0]}", context{js})', 2))
        else:
            exprs.append(tab("expr = [", 2))
            for line in expr:
                exprs.append(tab(f'"{line}",', 3))
            exprs.append(tab("]", 2))
            exprs.append(tab(f'return js_eval(expr, context{js})', 2))
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
                if is_expr(arg):
                    var_cast = True
                    arg = add_expr_function(multiline_to_list(arg), n_func)
                    n_func += 1
                else:
                    arg = f'"{arg}"'
                ordered_items.append((0, i, arg, False, None, var_cast))
            elif isinstance(arg, CommandLineBinding):
                # valueFrom = normalize(arg.valueFrom)
                if is_expr(arg.valueFrom):
                    var_cast = True
                    valueFrom = add_expr_function(multiline_to_list(arg.valueFrom), n_func)
                    n_func += 1
                else:
                    valueFrom = f'"{normalize(arg.valueFrom)}"'

                # Read the position attribute. If 'None' is found, set default
                pos = getattr(arg, "position", 0)
                if pos is None: 
                    pos = 0
                ordered_items.append((pos, i, valueFrom, False, arg, var_cast))
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
            valueFrom = binding.valueFrom
            if is_expr(valueFrom):
                value_or_expr = add_expr_function(multiline_to_list(valueFrom), n_func)
                n_func += 1
            else:
                var_cast = False
                value_or_expr = f'"{valueFrom}"'
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
        exprs: list[str],
        js: str
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
            IM.add_from(SDK, "js_eval")
            envValue = multiline_to_list(envValue)
            exprs.append(tab(f'def env_{var.envName}(context):'))
            if len(envValue) == 1:
                exprs.append(tab(f'return js_eval({envValue[0]}, context{js})', 2))
            else:
                exprs.append(tab("expr = [", 2))
                for line in envValue:
                    exprs.append(tab(f'"{line}",', 3))
                exprs.append(tab("]", 2))
                exprs.append(tab(f'return js_eval(expr, context{js})', 2))
            return f'env_{var.envName}(tool_context)'
        return f'"{envValue}"'

    # Parse stdin, stdout, stderr
    if exists(tool, "stdin"):
        if is_expr(tool.stdin):
            IM.add_from(SDK, "js_eval")
            stdin = multiline_to_list(tool.stdin)
            exprs.append(tab("def stdin_handler(context):"))
            if len(stdin) == 1:
                exprs.append(tab(f'return js_eval("{stdin[0]}", context{js})', 2))
            else:
                exprs.append(tab("expr = [", 2))
                for line in stdin:
                    exprs.append(tab(f'"{line}",', 3))
                exprs.append(tab("]", 2))
                exprs.append(tab(f'return js_eval(expr, context{js})', 2))
            stdin = "stdin_handler(tool_context)"
        else:
            stdin = f'"{stdin}"'

        lines.append(tab(f'stdin = open({stdin}, "r")'))
        run_lines.append("stdin=stdin")
        clean_up.append(tab(f'stdin.close()'))

    stdout = uses_stdout()
    if stdout:
        if is_expr(stdout):
            IM.add_from(SDK, "js_eval")
            stdout = multiline_to_list(tool.stdout)
            exprs.append(tab("def stdout_handler(context):"))
            if len(stdout) == 1:
                exprs.append(tab(f'return js_eval("{stdout[0]}", context{js})', 2))
            else:
                exprs.append(tab("expr = [", 2))
                for line in stdout:
                    exprs.append(tab(f'"{line}",', 3))
                exprs.append(tab("]", 2))
                exprs.append(tab(f'return js_eval(expr, context{js})', 2))
            stdout = "stdout_handler(tool_context)"
        else:
            stdout = f'"{stdout}"'

        lines.append(tab(f'stdout = open({stdout}, "w")'))
        run_lines.append("stdout=stdout")
        clean_up.append(tab(f'stdout.close()'))

    if exists(tool, "stderr"):
        if is_expr(tool.stderr):
            IM.add_from(SDK, "js_eval")
            stderr = multiline_to_list(tool.stderr)
            exprs.append(tab("def stderr_handler(context):"))
            if len(stderr) == 1:
                exprs.append(tab(f'return js_eval("{stderr[0]}", context{js})', 2))
            else:
                exprs.append(tab("expr = [", 2))
                for line in stderr:
                    exprs.append(tab(f'"{line}",', 3))
                exprs.append(tab("]", 2))
                exprs.append(tab(f'return js_eval(expr, context{js})', 2))
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
        exprs: list[str],
        js: str
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
                # Expression
                IM.add_from(SDK, "js_eval")
                g = multiline_to_list(g)
                if len(g) == 1:
                    exprs.append(tab(f'pattern = js_eval("{g[0]}", context{js})', 2))
                else:
                    exprs.append(tab("expr = [", 2))
                    for line in g:
                        exprs.append(tab(f'"{line}",', 3))
                    exprs.append(tab("]", 2))
                    exprs.append(tab(f'pattern = js_eval(expr, context{js})', 2))
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
            
        outputEval = multiline_to_list(binding.outputEval)
        if len(outputEval) == 1:
            exprs.append(tab(f'return js_eval("{outputEval[0]}", context{js})', 2))
        else:
            exprs.append(tab("expr = [", 2))
            for line in outputEval:
                exprs.append(tab(f'"{line}",', 3))
            exprs.append(tab("]", 2))
            exprs.append(tab(f'return js_eval(expr, context{js})', 2))

    else:
        p = "" if t.is_array else "[0]"
        exprs.append(tab(f"return {t.types}({x}{p})", 2))

    return tab(f'"{id}": outputs_{id}(tool_context),', 2)


def parse_tool(tool: CommandLineTool) -> list[str]:
    header:  list[str] = []
    exprs:   list[str] = []
    inputs:  list[str] = []
    iwr:     list[str] = [] # InitialWorkdirRequirement 
    command: list[str] = []
    outputs: list[str] = []

    # Get requirements
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

    # Insert InlineJavascriptRequirement before expression functions so we can
    # omit access the code without providing it via function parameters.
    # BUG FIXME Somehow parse ($include: ...) statements, currently bugged.
    js = ""
    if ("InlineJavascriptRequirement" in requirements and
        exists(requirements["InlineJavascriptRequirement"], "expressionLib")
        ):
        js = ", js_context"
        expressionLib = requirements["InlineJavascriptRequirement"].expressionLib
        header.extend(parse_js_req(expressionLib))

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

    # Insert InitialWorkdirRequirment #TODO activate
    if "InitialWorkDirRequirement" in requirements:
        # print(tab("InitialWorkdirRequirement parsing is not yet supported!"))
        iwr.extend(comment(tab("# Stage files and directories to the temporary working directory")))
        iwr.extend(parse_init_work_dir_req(
                        requirements["InitialWorkDirRequirement"],
                        exprs, js))

    # Parse command
    command.extend(comment(tab("# Ready the commandline and execute the tool")))
    command.extend(parse_commandline(tool, exprs, js))
    command.extend(parse_run(tool, requirements, exprs, js))
    command.append("")

    # Parse outputs
    outputs.extend(comment(tab("# Collect and generate outputs")))
    outputs.append(tab("return {"))
    for o in tool.outputs:
        outputs.append(parse_tool_output_binding(o, exprs, js))
    outputs.append(tab("}"))

    # Remove tool_context statement if no expressions are used
    if len(exprs) == 0:
        inputs.pop(context_pos - 1)
    exprs.append("")

    return header + exprs + inputs + iwr + command + outputs


def extract_source(source: str):
    keys = source.split("#")[-1].split("/")
    if len(keys) == 2:
        # Source is a workflow input: process_id/input_id
        return f'inputs["{keys[1]}"]'
    else: # Source is other step input: process_id/step_id/input_id
        return f'{keys[1]}_out["{keys[2]}"]'

        
def parse_workflow_step_inputs(
        step: WorkflowStep, 
        step_id: str
    ) -> list[str]:
    """
    
    """
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
                        if not use_linkMerge:
                            source = f'{pickValue}([{source}])'
                        else:
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


def parse_workflow_step(
        step: WorkflowStep, 
        exprs: list[str],
        js: str
    ) -> list[str]:
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
                    IM.add_from(SDK, "js_eval")

                    if exists(input, "source") or exists(input, "default"):
                        exprs.append(tab(f"def {step_id}_{input_id}(context):"))
                        lines.append(tab(f'{input_dict}["{input_id}"] = {step_id}_{input_id}(wf_context | {{"self": {step_id}_in["{input_id}"]}})', tabs))
                    else:
                        exprs.append(tab(f"def {step_id}_{input_id}(context):"))
                        lines.append(tab(f'{input_dict}["{input_id}"] = {step_id}_{input_id}(wf_context)', tabs))
                    
                    expr = multiline_to_list(input.valueFrom)
                    if len(expr) == 1:
                        exprs.append(tab(f'return js_eval("{expr[0]}", context{js})', 2))
                    else:
                        exprs.append(tab("expr = [", 2))
                        for line in expr:
                            exprs.append(tab(f'"{line}",', 3))
                        exprs.append(tab("]", 2))
                        exprs.append(tab(f'return js_eval(expr, context{js})', 2))
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
        IM.add_from(SDK, "js_eval")
        x = 1
        when = multiline_to_list(step.when)
        exprs.append(tab(f'def {step_id}_when(context):'))
        if len(when) == 1:
            exprs.append(tab(f'return js_eval("{when[0]}", context{js})', 2))
        else:
            exprs.append(tab("expr = [", 2))
            for line in when:
                exprs.append(tab(f'"{line}",', 3))
            exprs.append(tab("]", 2))
            exprs.append(tab(f'return js_eval(expr, context{js})', 2))

        lines.append(tab(f'wf_context["inputs"] = {step_id}_in'))
        lines.append(tab(f'if {step_id}_when(wf_context):'))

    # Parse step context and execution
    subprocess_id = step.subprocess.id.split("#")[-1]
    use_valueFrom = any(exists(i, "valueFrom") for i in step.in_)
    if exists(step, "scatter"):
        IM.add_from(SDK, "scatterizer")
        IM.add_from(SDK, "transpose")
        lines.append(tab(f'{step_id}_scattered_out = []', 1 + x))
        lines.append(tab(f'for scattered_inputs in scatterizer({step_id}_in, "input"):', 1 + x))
        lines.append(tab(f'wf_context["inputs"] = inputs | scattered_inputs', 2 + x))
        lines.extend(parse_valueFrom(True, 2 + x))
        lines.append(tab(f'{step_id}_scattered_out.append({subprocess_id}(scattered_inputs, context))', 2 + x))
        lines.append(tab(f"{step_id}_out = dask.delayed(transpose)({step_id}_scattered_out)", 1 + x))
    else:
        if use_valueFrom:
            lines.append(tab(f'wf_context["inputs"] = inputs | {step_id}_in'))
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
    use_linkMerge = False
    use_pickValue = False
    linkMerge = "merge_nested"
    if exists(output, "linkMerge"):
        if output.linkMerge not in ("merge_nested", "merge_flattened"):
            raise ValueError("Expected 'merge_nested' or merge_flattened, but got", output.linkMerge)
        use_linkMerge = True
        linkMerge = output.linkMerge
        IM.add_from(SDK, linkMerge)
    if exists(output, "pickValue"):
        if output.pickValue not in ('first_non_null', 'the_first_non_null', 'all_non_null'):
            raise ValueError("Expected 'first_non_null', 'the_first_non_null', or 'all_non_null', but got", output.pickValue)
        use_pickValue = True
        pickValue = output.pickValue
        IM.add_from(SDK, pickValue)

    source = output.outputSource
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
                if not use_linkMerge:
                    source = f'{pickValue}([{source}])'
                else:
                    source = f'{pickValue}({source})'
    else:
        # Single source
        source = extract_source(source) 

    output_id = output.id.split("/")[-1]
    return tab(f'"{output_id}": {source},', 2)
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

    # Get requirements
    requirements: dict[str, Any] = {}
    if exists(wf, "requirements"):
        requirements = {str(req.class_): req for req in wf.requirements}

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

    # Insert InlineJavascriptRequirement before expression functions so we can
    # omit access the code without providing it via function parameters.
    # BUG FIXME Somehow parse ($include: ...) statements, currently bugged.
    js = ""
    if ("InlineJavascriptRequirement" in requirements and
        exists(requirements["InlineJavascriptRequirement"], "expressionLib")
        ):
        js = ", js_context"
        expressionLib = requirements["InlineJavascriptRequirement"].expressionLib
        header.extend(parse_js_req(expressionLib))

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
    inputs.append(tab('wf_context = {"inputs": inputs} | context'))
    # context_pos = len(inputs)
    inputs.append("")

    # Parse steps
    for step in wf.steps:
        steps.extend(parse_workflow_step(step, exprs, js))

    # Parse outputs
    outputs.extend(comment(tab("# Compute outputs")))
    outputs.append(tab("return {"))
    for output in wf.outputs:
        outputs.append(parse_workflow_output(output))
    outputs.append(tab("}"))


    # # Remove wf_context statement if no expressions are used
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
    ls.append(tab('print(*[f"{k}: {v}" for k, v in result.items()], sep="\\n")'))
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
