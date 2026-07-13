import argparse, os

from pathlib import Path
from types import NoneType
from typing import (
    Optional
)

from utils import FileObject, DirectoryObject

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
    WorkflowStepOutput,
)


# Whether to use the default Dask Client or jobqueue SLURM client
SLURM = False

# Whether code comments will be added to the script
COMMENTS = False

# Cache all processes, indexed by their path
PROCS: dict[str, list[str]] = {}


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
    return s.startswith("$(") and s.endswith(")")


class ImportManager:
    imports: set
    from_imports: dict[str, set]

    def __init__(self):
        self.imports = set()
        self.from_imports = {}

        self.add("dask")
        self.add("subprocess")
        self.add("sys")
        self.add("yaml")
        self.add_from("dask.distributed", "Client")

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
}

class CWLType:
    is_array: bool
    optional: bool
    types: str | list[str]

    def __init__(self, type_):
        """
        """
        if isinstance(type_, (CommandInputArraySchema, InputArraySchema)):
            self.optional = False
            self.is_array = True
            type_ = type_.items
            self.types = T_MAPPING["".join([c.lower() for c in str(type_) if c not in ["?[]"]])]
        elif isinstance(type_, str):
            self.optional = "?" in type_
            self.is_array = "[]" in type_
            self.types = T_MAPPING["".join([c.lower() for c in type_ if c not in ["?[]"]])]
        elif isinstance(type_, list):
            # TODO Support optional and multitypes
            # Union of types, can also be optional
            print(tab("Input binding has multiple types, which is not supported yet."))
            print(tab(f"Selecting the first found type as input type instead."))
            type_ = type_[0]
            self.optional = "?" in type_
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
    # "null": "NoneType",
    # "boolean": "bool",
    # "int": "int",
    # "long": "int",
    # "float": "float",
    # "double": "float",
    # "string": "str",
    # "file": "FileObject",
    # "directory": "DirectoryObject",
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


def gather_processes(path: Path, processes: dict[str, Process]) -> None:
    # Index by the absolute file path to prevent duplicates
    path = path.resolve()
    if path in processes:
        return
    
    process = load_document_by_uri(path)
    processes[path] = process

    if isinstance(process, Workflow):
        for step in process.steps:
            step_path = Path(step.run[step.run.find(":") + 1:])
            # step_path = Path(step.run.(":")[])
            if not step_path.is_absolute():
                print(path)
                print(step_path)
                
                step_path = path / step_path
            gather_processes(step_path, processes)
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
            # case "bool":
            case "bool" | "int" | "float": 
                value = default
            case "str":
                value = f'"{default}"'
            case "FileObject":
                IM.add_from("utils", "FileObject")
                value = [f'"{k}":"{v}"' for k, v in default.items() if k in FILE_KEYS]
                value = f'FileObject({{{", ".join(value)}}})'
                
            case "DirectoryObject":
                IM.add_from("utils", "DirectoryObject")
        return value

    if cwl_type.is_array:
        return [parse_item(d) for d in default]
    else:
        return parse_item(default)


def parse_tool_input_parameter(input: CommandInputParameter) -> list[str]:
    """
    """
    id = input.id.split("/")[-1]
    cwl_type = CWLType(input.type_)

    if exists(input, "default"):
        default = parse_default(input.default, cwl_type)
    else:
        return [tab(f'"{id}": None,', 2)]
    
    if isinstance(default, str):
        return [tab(f'"{id}": {default},', 2)]
    else:
        return [
            tab(f'"{id}": [', 2),
            *[tab(f'{d},', 3) for d in default],
            tab("],", 2)
        ]


def parse_commandline(
        tool: CommandLineTool, 
        exprs: list[str]
    ) -> list[str]:
    """
    TODO Handle input valueFrom
    TODO Handle arrays

    Generate a Python list that holds the commandline-building statements for 
    `tool`. Any expression handlers generated are added to `exprs`.

    NOTE: Only accept an integer as inputBinding.position value
    """

    def add_expression_function(expression: str) -> str:
        global IM
        IM.add_from("utils", "js_eval")
        func_name = f"expr_handler_{len(exprs)}"
        exprs.append(tab(f"def {func_name}(context: dict) -> str:"))
        exprs.append(tab(f"return {expression}", 2))
        return f"{func_name}(tool_context)"

    def compose_cmd_arg(
            value_expr: str,
            is_array: bool,
            binding: Optional[CommandLineBinding] = None,
        ) -> str:
        # TODO Remove 'str(X)' when arg type is string
        prefix = getattr(binding, "prefix", "")
        separate = getattr(binding, "separate", True)
        itemSeparator = getattr(binding, "itemSeparator", None)
        if is_array:
            if itemSeparator:
                if prefix and separate:         # -i= A,B,C
                    arg = f'{prefix}, '
                    arg += f'{itemSeparator}.join(str(x) for x in {value_expr})'
                elif prefix and not separate:   # -i=A,B,C
                    arg = f'{prefix}'
                    arg += f'{itemSeparator}.join(str(x) for x in {value_expr})'
                else:                           # A,B,C
                    arg = f'{itemSeparator}.join(str(x) for x in {value_expr})' 
            else:
                if prefix and separate:         # -i= A B C
                    arg = f'{prefix}, '
                    arg += f'*[str(v) for v in {value_expr}]'
                if prefix and not separate:     # -i=A -i=B -i=C
                    arg = f'*[{prefix} + str(v) for v in {value_expr}]'
                else:                           # A B C
                    arg = f'*[str(v) for v in {value_expr}]'
        else:
            if prefix:
                if separate:
                    arg = f"{prefix}, str({value_expr})"
                else:
                    arg = f"{prefix} + str({value_expr})"
            else:
                arg = f"str({value_expr})"
        return arg

    # Each tuple stores:
    # (position, argument index, value expression, is-array, binding object)
    ordered_items: list[tuple[int, int, str, bool, object | None]] = []

    # Assign a sorting key (inputBinding.position, argument index) to the tool
    # arguments.
    # TODO Handle arg valueFrom typing?
    if exists(tool, "arguments"):
        for i, arg in enumerate(tool.arguments):
            if isinstance(arg, str):
                if is_expr(arg):
                    arg = add_expression_function(arg[2:-1])
                ordered_items.append((0, i, arg, False, None, False, None))
            elif isinstance(arg, CommandLineBinding):
                value_expr = arg.valueFrom
                if is_expr(value_expr):
                    value_expr = add_expression_function(value_expr[2:-1])
                pos = getattr(arg, "position", 0)
                ordered_items.append((pos, i, value_expr, False, arg))
            else:
                raise TypeError(f"Unsupported argument type: {type(arg)}")
 
    # TODO Handle input valueFrom typing?
    for input_ in tool.inputs:        
        if not exists(input_, "inputBinding"):
            continue

        input_id = input_.id.split("/")[-1]
        binding = input_.inputBinding
        t = CWLType(input_.type_)
        pos: int = getattr(binding, "position", 0)
        value_expr = f'inputs["{input_id}"]'

        # If the binding has valueFrom, add a expression handler if needed
        if exists(binding, "valueFrom"):
            value_expr = binding.valueFrom
            if is_expr(value_expr):
                value_expr = add_expression_function(value_expr[2:-1])
            else:
                value_expr = f'"{value_expr}"'
        ordered_items.append((pos, len(ordered_items), value_expr,
                              t.is_array, binding))

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
    
    # Sort and apply the commandline bindings
    ordered_items.sort(key=lambda item: (item[0], item[1]))
    for _, _, value_expr, is_array, binding in ordered_items:
        command_items.append(compose_cmd_arg(value_expr, is_array, binding))

    lines = [tab("cmd = [")]
    for item in command_items:
        lines.append(tab(f"{item},", 2))
    lines.append(tab("]"))
    return lines


def parse_tool_output_binding(
        output: CommandOutputParameter, 
        exprs: list[str]
    ) -> str:
    """
    TODO 
    Return an output assignment for a CWL output.

    NOTE: Output must have outputBinding, which is only not the case when the
    output type is stdout.
    """
    global IM
    id = output.id.split("/")[-1]
    t = CWLType(output.type_)

    if "FileObject" in t.types:
        IM.add_from("utils", "FileObject")
    if "DirectoryObject" in t.types:
        IM.add_from("utils", "DirectoryObject")

    # Create expression handler that takes handles an output's glob matching
    # and outputEval.
    binding = output.outputBinding
    exprs.append(tab(f"def outputs_{id}(context):"))
    glob_flag = False
    if exists(binding,"glob"):
        glob_flag = True
        g = binding.glob
        IM.add_from("utils", "glob")
        if isinstance(g, str):
            if is_expr(g):
                # Expression
                IM.add_from("utils", "js_eval")
                exprs.append(tab(f'pattern = js_eval("{g[2:-1]}", context)', 2))
                x = "glob(pattern)"
            else:
                # Simple string
                x = f'glob("{g}")'
        else:
            # List of simple strings
            patterns = ", ".join([f'"{p}"' for p in g])
            exprs.append(tab(f'pattern = [{patterns}]'), 2)
            g = "glob(pattern)"

    if exists(binding, "outputEval"):
        if glob_flag:
            exprs.append(tab(f'matches = {x}'))
            exprs.append(tab(f'context["self"] = [FileObject(m) for m in matches]'), 2)
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

    # header
    tool_id = tool.id.split("#")[-1]
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
    # Parse default values
    inputs.append(tab("inputs = {"))
    for i in tool.inputs:
        inputs.extend(parse_tool_input_parameter(i))
    inputs.append(tab("}"))
    inputs.append(tab("inputs.update(input_obj)"))
    inputs.append(tab('tool_context = {"inputs": inputs, **context}'))
    context_pos = len(inputs)
    inputs.append("")

    # Parse command
    command.extend(comment(tab("# Ready the commandline and execute the tool")))
    command.extend(parse_commandline(tool, exprs))
    command.append(tab('print("Running:",  *cmd)'))
    command.append(tab("subprocess.run(cmd)"))
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


# def parse_workflow_input_parameter(input) -> list[str]:
#     """
#     """
#     id = input.id.split("/")[-1]
#     cwl_type = CWLType(input.type_)

#     if exists(input, "default"):
#         default = parse_default(input.default, cwl_type)
#     else:
#         return [tab(f'"{id}": None,', 2)]
    
#     if isinstance(default, str):
#         return [tab(f'"{id}": {default},', 2)]
#     else:
#         return [
#             tab(f'"{id}": [', 2),
#             *[tab(f'{d},', 3) for d in default],
#             tab("],", 2)
#         ]


def parse_workflow_step(step, exprs) -> list[str]:
    lines = []
    step_id = step.id.split("/")[-1]
    
    
    # Parse step metadata
    lines.append(tab(f'# Step ID:    {step_id}'))
    if exists(step, "label"):
        lines.append(tab(f'# Step label: {step.label}'))

    # Parse step inputs (source/default/valueFrom)
    lines.append(tab(f'{step_id}_in = {{'))
    for input in step.in_:
        input_id = input.id.split("/")[-1]

        if exists(input, "default"):
            default = parse_default(input.default, convert_to_CWLType(input.default))
        
        if exists(input, "source"):
            source = input.source
            if isinstance(source, list):
                if len(source) > 1:
                    raise Exception("Multisourcing not supported")
                source = source[0]

            keys = source.split("#")[-1].split("/")
            if len(keys) == 2:
                # Source is a workflow input: process_id/input_id
                source = f'inputs["{keys[1]}"]'
            else: # Source is other step input: process_id/step_id/input_id
                source = f'{keys[1]}_out["{keys[2]}"]'

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
        

    # Parse step context and execution
    if exists(step, "scatter"):
        ...
    else:
        ...
        

    lines.append("")
    return lines


def parse_workflow_output_binding(
        output: WorkflowOutputParameter, 
        exprs: list[str]
    ) -> str:
    return


def parse_workflow(wf: Workflow):
    header:  list[str] = []
    exprs:   list[str] = []
    inputs:  list[str] = []
    steps:   list[str] = []
    outputs: list[str] = []

    # header
    wf_id = wf.id.split("#")[-1]
    header.append('@dask.delayed')
    header.append(f'def {wf_id}(input_obj: dict, context: dict) -> dict:')
    
    # Metadata
    header.append(tab('"""'))
    header.append(tab('class: Workflow'))
    if exists(wf, "label"):
        header.append(tab('label: ' + wf.label))
    header.append(tab('"""'))

    # Input object to inputs
    inputs.extend(comment(tab("# Gather inputs in their correct format")))
    # Parse default values
    inputs.append(tab("inputs = {"))
    for i in wf.inputs:
        inputs.extend(parse_tool_input_parameter(i))
    inputs.append(tab("}"))
    inputs.append(tab("inputs.update(input_obj)"))
    inputs.append(tab('tool_context = {"inputs": inputs, **context}'))
    context_pos = len(inputs)
    inputs.append("")

    # Parse steps
    for step in wf.steps:
        steps.extend(parse_workflow_step(step, exprs))

    # Parse outputs
    outputs.extend(comment(tab("# Compute outputs")))

    # Remove tool_context statement if no expressions are used
    if len(exprs) == 0:
        inputs.pop(context_pos - 1)
    exprs.append("")
        
    return header + exprs + inputs + steps  + outputs


def parse_main(main_id: str) -> list[str]:
    """
    Create the script main entry.
    """
    m_ls: list[str] = ["", "def main():"]

    # Write DASK client initialization
    m_ls.extend(comment(tab("# Initialize cluster")))
    if SLURM:
        m_ls.extend(comment(tab("# NOTE: Memory argument is forced by the SLURMCluster ")))
        m_ls.extend(comment(tab("# initializer. This causes problems on systems that disable")))
        m_ls.extend(comment(tab("# setting memory requirements (DAS6 has this restriction). The")))
        m_ls.extend(comment(tab("# band-aid is to ignore the memory setting line with")))
        m_ls.extend(comment(tab("# 'job_directives_skip'.")))
        m_ls.append(tab('cluster = SLURMCluster('))
        m_ls.append(tab('cores=16,', 2))
        m_ls.append(tab('memory="16GB",', 2))
        m_ls.append(tab('walltime="00:15:00",', 2))
        m_ls.append(tab('job_directives_skip=[\'--mem\']', 2))
        m_ls.append(tab(")"))
        m_ls.append(tab("cluster.scale(4)"))
        m_ls.append(tab("client = Client(cluster)", 1))
    else:
        m_ls.append(tab("client = Client()"))

    m_ls.append("")
    m_ls.extend(comment(tab("# Convert input YAML to dict")))
    m_ls.append(tab('with open(sys.argv[1], "r") as f:'))
    m_ls.append(tab("input_yaml = yaml.load(f, Loader=yaml.BaseLoader)", 2))
    m_ls.append("")
    m_ls.extend(comment(tab("# Initialize CWL context")))
    m_ls.append(tab("context = {}"))
    m_ls.append("")
    m_ls.extend(comment(tab("# Submit to DASK")))
    m_ls.append(tab(f"result = client.compute({main_id}(input_yaml, context)).result()"))
    m_ls.append(tab("print(*[f'{k}: {v}' for k, v in result.items()])"))
    m_ls.append("")
    m_ls.append('if __name__ == "__main__":')
    m_ls.append(tab("main()"))
    return m_ls


def parse_cwl(cwl_path):
    global IM
    body_lines = []
    processes = {}

    # Gather all unique procesess in preorder fashion
    gather_processes(cwl_path, processes)

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
    
    with open(output_path, "w") as output_file:
        lines = parse_cwl(cwl_path)
        output_file.writelines([f'{l}\n' for l in lines])


if __name__ == "__main__":
    main()
