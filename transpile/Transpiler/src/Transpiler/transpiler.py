import argparse, os

from pathlib import Path
from types import NoneType

from cwl_utils.parser import (
    load_document_by_uri,
    CommandLineTool,
    ExpressionTool,    
    Process,
    Workflow,
)
from cwl_utils.parser.cwl_v1_2 import (
    CommandOutputArraySchema, 
    CommandInputArraySchema,
    CommandLineBinding,
    Dirent,
    InputArraySchema,
    OutputArraySchema,
    WorkflowStepOutput,
)

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

# Whether to use the default Dask Client or jobqueue SLURM client
SLURM = False

# Whether code comments will be added to the script
COMMENTS = False

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

def tab(string: str, tab_amount: int = 1) -> str:
    """
    Apply a number of tabs to a string and return it.
    """
    return "\t" * tab_amount + string

def comment(string: str) -> list[str]:
    if COMMENTS:
        return [string]
    return []

def exists(o: object, key: str) -> bool:
    return hasattr(o, key) and getattr(o, key) is not None

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

class CWLType:
    is_array: bool
    optional: bool
    types: str | list[str]
    # default: str

    def __init__(self, type_, id: str):
        """
        Parse input types and transform to PWF format.
        Returns:
            List of strings representing lines to write to file.
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
            print(tab(f"Input binding '{id}' has multiple types, which is not supported yet."))
            print(tab(f"Selecting the first found type as input type instead."))
            type_ = type_[0]
            self.optional = "?" in type_
            self.is_array = "[]" in type_
            self.types = T_MAPPING["".join([c.lower() for c in type_ if c not in ["?[]"]])]
        else:
            raise NotImplementedError(f"Found unsupported type {type(type_)}")


def parse_input_binding(binding, exprs: list[str]) -> str:
    """
    TODO Support for default values
    TODO Support expressions
    """
    global IM
    id = binding.id.split("/")[-1]
    t = CWLType(binding.type_, id)
    # Add import dependency if needed
    if "FileObject" in t.types:
        IM.add_from("utils", "FileObject")
    if "DirectoryObject" in t.types:
        IM.add_from("utils", "DirectoryObject")

    if t.is_array:
        rhs = f'[{t.types}(f) for f in input_obj["{id}"]]'
    else:
        rhs =  f'{t.types}(input_obj["{id}"])'
    return tab(f'inputs["{id}"] = {rhs}')


def parse_commandline(tool, exprs: list[str]) -> list[str]:
    """
    TODO Triple check clanker code
    
    Generate a static Python list literal for the command line.
    """
    global IM
    command_items: list[str] = []

    def add_expression_function(expression: str) -> str:
        func_name = f"expr_{len(exprs)}"
        exprs.append(tab(f"def {func_name}(context: dict) -> str:"))
        exprs.append(tab(f"return {expression}", 2))
        return f"{func_name}(local_context)"

    def append_value(value_expr: str, is_array: bool, binding=None) -> None:
        prefix = getattr(binding, "prefix", "")
        separate = getattr(binding, "separate", True)
        item_separator = getattr(binding, "itemSeparator", None)

        if is_array:
            if item_separator:
                if prefix and separate:
                    command_items.append(prefix)
                    command_items.append(f'{item_separator}.join(str(v) for v in {value_expr})')
                elif prefix and not separate:
                    command_items.append(f'{prefix} + {item_separator}.join(str(v) for v in {value_expr})')
                else:
                    command_items.append(f'{item_separator}.join(str(v) for v in {value_expr})')
            else:
                if prefix and separate:
                    command_items.append(repr(prefix))
                    command_items.append(f'*[str(v) for v in {value_expr}]')
                elif prefix and not separate:
                    command_items.append(f'*[f"{prefix}{{str(v)}}" for v in {value_expr}]')
                else:
                    command_items.append(f'*[str(v) for v in {value_expr}]')
            return

        if prefix and separate:
            command_items.append(repr(prefix))
            command_items.append(f'str({value_expr})')
        elif prefix and not separate:
            command_items.append(f'{prefix} + str({value_expr})')
        else:
            command_items.append(f'str({value_expr})')

    if exists(tool, "baseCommand"):
        base_command = tool.baseCommand
        if isinstance(base_command, str):
            base_command = [base_command]
        elif not isinstance(base_command, list):
            raise TypeError(f"Unsupported baseCommand type: {type(base_command)}")

        for entry in base_command:
            if isinstance(entry, str):
                command_items.append(repr(entry))
            else:
                raise TypeError(f"Unsupported baseCommand entry type: {type(entry)}")

    # Each tuple stores: (position, original-order index, value expression, is-array, binding object)
    ordered_items: list[tuple[int, int, str, bool, object | None]] = []

    for input_ in tool.inputs:
        if not exists(input_, "inputBinding"):
            continue

        binding = input_.inputBinding
        input_id = input_.id.split("/")[-1]
        t = CWLType(input_.type_, input_id)
        position = int(getattr(binding, "position", 0))
        ordered_items.append((position, len(ordered_items), f'inputs["{input_id}"]', t.is_array, binding))

    if exists(tool, "arguments"):
        for arg in tool.arguments:
            if isinstance(arg, str):
                ordered_items.append((0, len(ordered_items), repr(arg), False, None, True, None))
            elif isinstance(arg, CommandLineBinding):
                value_from = getattr(arg, "valueFrom", None)
                if isinstance(value_from, str):
                    if value_from.startswith("$(") and value_from.endswith(")"):
                        IM.add_from("utils", "js_eval")
                        value_expr = add_expression_function(f'js_eval({value_from!r}, context)')
                    else:
                        value_expr = repr(value_from)
                else:
                    value_expr = repr(value_from)
                position = int(getattr(arg, "position", 0))
                ordered_items.append((position, len(ordered_items), value_expr, False, arg))
            else:
                raise TypeError(f"Unsupported argument type: {type(arg)}")

    ordered_items.sort(key=lambda item: (item[0], item[1]))
    for _, _, value_expr, is_array, binding in ordered_items:
        append_value(value_expr, is_array, binding)

    lines = [tab("cmd = [")]
    for item in command_items:
        lines.append(tab(f"{item},", 2))
    lines.append(tab("]"))
    return lines


def parse_output_binding(binding, exprs: list[str]) -> str:
    """
    TODO Improve, generalize
    Emit a simple output assignment for a CWL output binding.
    """
    global IM
    id = binding.id.split("/")[-1]
    t = CWLType(binding.type_, id)

    if "FileObject" in t.types:
        IM.add_from("utils", "FileObject")
    if "DirectoryObject" in t.types:
        IM.add_from("utils", "DirectoryObject")

    if exists(binding, "outputBinding") and exists(binding.outputBinding, "glob"):
        g = binding.outputBinding.glob
        IM.add_from("glob", "glob")
        exprs.append(tab(f"def outputs_{id}_glob(context: dict):"))
        if isinstance(g, str):
            if g.startswith("$(") and g.endswith(")"):
                IM.add_from("utils", "js_eval")
                exprs.append(tab(f'return js_eval({g!r}, context)', 2))
            else:
                exprs.append(tab(f'return {g!r}', 2))
        elif isinstance(g, list):
            exprs.append(tab(f'return {g}', 2))
        else:
            raise TypeError(type(g))
        rhs = f'[{t.types}(f) for f in glob(outputs_{id}_glob(local_context))]' if t.is_array else f'{t.types}(glob(outputs_{id}_glob(local_context))[0])'
    else:
        rhs = "None"
    return tab(f'outputs["{id}"] = {rhs}')

def parse_tool(tool: CommandLineTool):
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

    # expression callbacks
    # Input object to inputs
    inputs.extend(comment(tab("# Gather inputs in their correct format")))
    inputs.append(tab("inputs = {}"))
    for i in tool.inputs:
        inputs.append(parse_input_binding(i, exprs))
    inputs.append(tab('local_context = {"inputs": inputs, **context}'))
    inputs.append("")

    # command
    inputs.extend(comment(tab("# Ready the commandline and its arguments")))
    command.extend(parse_commandline(tool, exprs))
    command.append(tab('print("Running:",  *cmd)'))
    command.append(tab("subprocess.run(cmd)"))
    command.append("")

    # outputs
    inputs.extend(comment(tab("# Collect and generate outputs")))
    command.append(tab("outputs: dict = {}"))
    for o in tool.outputs:
        outputs.append(parse_output_binding(o, exprs))
    outputs.append(tab("return outputs"))

    exprs.append("")
    return header + exprs + inputs + command + outputs


def parse_workflow(wf: Workflow):
    something = []
    return something


def parse_process(cwl):
    lines = []

    if isinstance(cwl, ExpressionTool):
        raise NotImplementedError("ExpressionTool transpilation is not supported")
    
    if isinstance(cwl, CommandLineTool):
        lines.extend(parse_tool(cwl))
    elif isinstance(cwl, Workflow):
        lines.extend(parse_workflow(cwl))
    else:
        raise TypeError("Unsupported CWL Process type", type(cwl))
    return lines


def parse_cwl(cwl):
    global IM
    cwl_id = cwl.id.split("#")[-1]
    body_lines = parse_process(cwl)
    
    # Create script main entry
    m_ls: list[str] = ["def main():"]
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
    m_ls.append(tab(f"result = client.compute({cwl_id}(input_yaml, context)).result()"))
    m_ls.append(tab("print(*[f'{k}: {v}' for k, v in result.items()])"))
    m_ls.append("")
    m_ls.append('if __name__ == "__main__":')
    m_ls.append(tab("main()"))


    return IM.get_lines() + body_lines + m_ls


def main():
    arg_parser = create_arg_parser()
    args = arg_parser.parse_args()

    cwl_path = Path(args.input)
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


    # Load CWL process into an object
    cwl = load_document_by_uri(cwl_path)

    # Expression tools are extracted as normal tools
    if isinstance(cwl, ExpressionTool):
        raise NotImplementedError("ExpressionTool transpilation is not supported")
    
    with open(output_path, "w") as output_file:
        lines = parse_cwl(cwl)
        output_file.writelines([f'{l}\n' for l in lines])


if __name__ == "__main__":
    main()
