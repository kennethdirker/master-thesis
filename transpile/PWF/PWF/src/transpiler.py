import argparse
import glob
import os
import textwrap

from pathlib import Path
from typing import Any, List, Mapping, Optional, TextIO, Tuple, Union

from cwl_utils.parser import load_document_by_uri

from cwl_utils.parser.cwl_v1_2 import (
    CommandOutputArraySchema, 
    CommandInputArraySchema,
    InputArraySchema,
    WorkflowStepOutput,
)

# Import CWL Process types
from cwl_utils.parser import (
    Process,
    CommandLineTool,
    Workflow,
    ExpressionTool
)

def indent(string: str, tab_amount: int) -> str:
    """
    Apply a number of tabs to a string and return it.
    """
    return "\t" * tab_amount + string


def format_dict_key_string(
        key: str,
        string: str,
        indents: int = 0,
        max_length: int = 80
    ) -> list[str]:
    """
    Format a long string as a dictionary key-value pair, splitting it into
    multiple lines if necessary. The first line will contain the key and the opening quote, indented for a certain number of tabs and the last line will contain the closing quote and comma, also indented. 

    Args:
        key: The dictionary key for the long string.
        string: The long string to format.
        indents: Number of tabs to indent each line.
        max_length: Maximum length of each line.
    Returns:
        List of strings.
    """
    if max_length <= 0:
        raise ValueError("max_length must be greater than 0")
    extra_length = 7  # for "{key}": "{value}",
    if indents * 4 + len(key) + len(string) + extra_length <= max_length:
        # The entire string fits on one line
        return [indent(f'"{key}": "{string}",', indents)]

    lines: list[str] = []
    lines.append(indent(f'"{key}": \\', indents))

    # Length of extra needed interpunction, eg. '"{LINE}" \'
    extra_length = 4  

    wrapped = textwrap.wrap(
        string, 
        width = max_length - (indents + 1) * 4 - extra_length,
        fix_sentence_endings=True)
    # Add quotes and trailing slash
    wrapped = ["\t" * (indents + 1) + f'"{l}" \\' for l in wrapped]
    # Remove last trailing slash
    wrapped[-1] = wrapped[-1][0:-2]
    lines.extend(wrapped)
    
    return lines


def normalize(string: str) -> str:
    """
    Normalize expressions by creating a single enclosed expression string.
    If the string does not contain expressions, the plain string is returned.
    Example:
        example_$(inputs.input).txt -> $('example_' + inputs.input + '.txt')
    Returns:
        The normalized string.
    """
    start = string.find("$(")
    if start < 0 or ")" not in string[start + 2:]:
        # String does not have an expression or not contain a
        # matching closing parenthesis
        return string
    
    count = string.count("$(")
    substrings: list[str] = []
    ss = string
    for _ in range(count):
        s, ss = ss.split("$(", 1)
        if len(s):
            # Here, s is always a plain string and needs quotation
            substrings.append(f"'{s}'")

        if ")" not in ss:
            # String does not contain a matching closing parenthesis
            # Add remaining string tail to last substring
            substrings[-1] = s + "$(" + ss
            break

        s, ss = ss.split(")", 1)
        # Here, s is always an expression, with ss being the remaining tail
        substrings.append(s)
    
    return '$(' + " + ".join(substrings) + ')'



def parse_prefix(
        cwl: Process,
        class_name: str,
        out_file: TextIO
    ) -> None:
    """
    Parse the prefix (class definition) and write to PWF file.
    """
    lines: list[str] = []

    if "CommandLineTool" in cwl.class_:
        lines.append("from PWF.src.commandlinetool import BaseCommandLineTool")
        lines.append("")
        lines.append(f"class {class_name}(BaseCommandLineTool):")
    elif "Workflow" in cwl.class_:
        lines.append("from PWF.src.workflow import BaseWorkflow")
        lines.append("")
        lines.append(f"class {class_name}(BaseWorkflow):")
    
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_metadata(
        cwl: Process,
        out_file: TextIO
    ) -> None:
    """
    Parse metadata and write to PWF file.
    """
    lines: list[str] = [""]
    lines.append(indent("def set_metadata(self):", 1))
    lines.append(indent("self.metadata = {", 2))
    
    # Label
    if hasattr(cwl, "label") and cwl.label is not None:
        lines.extend(format_dict_key_string("label", cwl.label, 3))
    # Doc
    if hasattr(cwl, "doc") and cwl.doc is not None:
        lines.extend(format_dict_key_string("doc", cwl.doc, 3))

    lines.append(indent("}", 2))

    # No metadata to write
    if len(lines) == 4:
        # Special case
        lines = [""]
        lines.append(indent("def set_metadata(self):", 1))
        lines.append(indent("self.metadata = {}", 2))
    

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def get_input_type(type_: Any) -> list[str]:
    """
    Parse input types and transform to PWF format.
    Returns:
        List of strings representing lines to write to file.
    """
    lines: list[str] = []

    if isinstance(type_, (CommandInputArraySchema, InputArraySchema)):
        lines.append(indent(f'"type": "{type_.items.lower()}[]",', 4))
    elif isinstance(type_, str):
        lines.append(indent(f'"type": "{type_.lower()}",', 4))
    elif isinstance(type_, List):
        # Union of types, can also be optional
        types: List[str] = type_.copy()

        if "null" in types:
            # Input is optional. Put 'null' at the end for clarity.
            types.remove("null")
            types.append("null")

        line = '"type": [' + ", ".join([f'"{t.lower()}"' for t in types]) + "],"
        lines.append(indent(line, 4))
    else:
        raise NotImplementedError(f"Found unsupported type {type(type_)}")

    return lines


def parse_inputs(
        cwl: Process,
        out_file: TextIO
    ) -> None:
    """
    Parse inputs and write to PWF file.
    """
    lines: list[str] = [""]
    lines.append(indent("def set_inputs(self):", 1))
    
    if hasattr(cwl, "inputs"):
        if len(cwl.inputs) == 0:
            # Special case: no inputs
            lines.append(indent("self.inputs = {}", 2))

            # Add newlines to each string
            lines = [line + "\n" for line in lines]
            out_file.writelines(lines)
            return

        # Make sure we are dealing with a list of input parameters
        inputs: List = []
        if isinstance(cwl.inputs, List):
            inputs = cwl.inputs
        elif isinstance(cwl.inputs, Mapping):
            # Output parameters are saved in a dict and must be transformed to
            # a list by including the output paramater ID as a key in the 
            # output parameter. 
            for id, output_dict in cwl.inputs:
                output_dict["id"] = id
                inputs.append(output_dict)
        else:
            raise Exception(f"Expected list/dict, but found '{type(cwl.inputs)}'")
        
        lines.append(indent("self.inputs = {", 2))
        for input in inputs:
            # Get input ID from file_path#[input_id | tool_id/input_id]
            # NOTE Force id field in cwl file?
            input_id = input.id.split("#")[-1].split("/")[-1]
            lines.append(indent(f'"{input_id}": {{', 3))

            # Type
            type_: list[str] = get_input_type(input.type_)
            lines.extend(type_)

            # Default
            if hasattr(input, "default") and input.default is not None:
                lines.append(indent(f'"default": "{input.default}",', 4))

            # secondaryFiles
            # TODO

            # Streamable
            # TODO

            # Inputbinding
            if hasattr(input, "inputBinding"):
                binding = input.inputBinding

                # Used to indicate that the input appears on the command line
                if binding is not None:
                    lines.append(indent('"bound": True,', 4))

                # Position
                if hasattr(binding, "position") and binding.position is not None:
                    lines.append(indent(f'"position": {binding.position},', 4))

                # Prefix
                if hasattr(binding, "prefix") and binding.prefix is not None:
                    lines.append(indent(f'"prefix": "{binding.prefix}",', 4))

                # valueFrom
                if hasattr(binding, "valueFrom") and binding.valueFrom is not None:
                    valueFrom = normalize(binding.valueFrom)
                    lines.append(indent(f'"valueFrom": "{valueFrom}",', 4))

            # loadContents
            # TODO

            # loadListing
            # TODO
            
            # Label
            if hasattr(input, "label") and input.label is not None:
                lines.extend(format_dict_key_string("label", input.label, 4))

            # Doc
            if hasattr(input, "doc") and input.doc is not None:
                lines.extend(format_dict_key_string("doc", input.doc, 4))

            lines.append(indent("},", 3))
        lines.append(indent("}", 2))

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def get_output_type(type_: Any) -> list[str]:
    """
    Parse output types and transform to PWF format.
    Returns:
        List of strings representing lines to write to file.
    """
    lines: list[str] = []

    if isinstance(type_, CommandOutputArraySchema):
        lines.append(indent(f'"type": "{type_.items.lower()}[]",', 4))
    elif isinstance(type_, str):
        lines.append(indent(f'"type": "{type_.lower()}",', 4))
    else:
        raise NotImplementedError(f"Found unsuppored type {type(type_)}")

    return lines


# def get_glob(glob: str) -> str:
#     """
    
#     """
#     # FIXME JS should not be transformed
#     s = glob
#     if "$(" in glob and ")" in glob:
#         s = glob[2:-1].split(".")
#         if "inputs" in s[0]:
#             # Get glob from input
#             glob = s[1]
#         else:
#             # Get glob from step
#             glob = "/".join(s[1:])
#         s = f"${glob}$"
#     return s


def parse_outputs(
        cwl: Process,
        out_file: TextIO
    ) -> None:
    """
    TODO Finish

    """
    lines: list[str] = [""]
    lines.append(indent("def set_outputs(self):", 1))
    lines.append(indent("self.outputs = {", 2))
    
    if hasattr(cwl, "outputs") and cwl.outputs is not None:
        # TODO Finish
        # TODO FIXME Examine the outputs, output object

        # Make sure we are dealing with a list of output parameters
        outputs: List = []
        if isinstance(cwl.outputs, List):
            outputs = cwl.outputs
        elif isinstance(cwl.outputs, Mapping):
            # Output parameters are saved in a dict and must be transformed to
            # a list by including the output paramater ID as a key in the 
            # output parameter. 
            for id, output_dict in cwl.outputs:
                output_dict["id"] = id
                outputs.append(output_dict)
        else:
            raise Exception(f"Expected list/dict, but found '{type(cwl.outputs)}'")
        
        for output in outputs:
            # ID
            # NOTE Force id field in cwl file?
            lines.append(indent(f'"{output.id.split("/")[-1]}": {{', 3))

            # Type
            type_: list[str] = get_output_type(output.type_)
            lines.extend(type_)
            
            if hasattr(output, "outputBinding"):
                binding = output.outputBinding
                # Glob
                if hasattr(binding, "glob") and binding.glob is not None:
                    glob = normalize(binding.glob)
                    lines.append(indent(f'"glob": "{glob}",', 4))
                # loadContents
                # TODO
                # outputEval
                if hasattr(binding, "outputEval") and binding.outputEval is not None:
                    outputEval = normalize(binding.outputEval)
                    lines.append(indent(f'"outputEval": "{outputEval}",', 4))
                # secondaryFiles
                # TODO
            # Streamable
            # TODO

            # Label
            if hasattr(output, "label") and output.label is not None:
                lines.extend(format_dict_key_string("label", output.label, 4))

            # Doc
            if hasattr(output, "doc") and output.doc is not None:
                lines.extend(format_dict_key_string("doc", output.doc, 4))

            lines.append(indent("},", 3))
    lines.append(indent("}", 2))

    # Special case: no outputs
    if len(lines) == 4:
        lines.clear()
        lines.append("")
        lines.append(indent("def set_outputs(self):", 1))
        lines.append(indent("self.outputs = {}", 2))

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_base_command(
        cwl: CommandLineTool,
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = [""]
    lines.append(indent("def set_base_command(self):", 1))
    lines.append(indent("self.base_command = [", 2))
    
    if hasattr(cwl, "baseCommand"):
        if not isinstance(cwl.baseCommand, list):
            cwl.baseCommand = [cwl.baseCommand]
        for command in cwl.baseCommand:
            lines.append(indent(f'"{command}",', 3))
        
    lines.append(indent("]", 2))

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_tool_requirements(
        cwl: CommandLineTool,
        out_file: TextIO
    ) -> None:
    """
    
    """
    # No requirements to parse
    if not hasattr(cwl, "requirements") or cwl.requirements is None or len(cwl.requirements) == 0:
        return
    # Only obsolite requirement, dont parse
    if len(cwl.requirements) == 1 and "InlineJavascriptRequirement" in cwl.requirements[0].class_:
        return
    
    def quote(value: Any) -> str:
        """ 
        Encase strings and string expressions in quotes and cast other types
        to string.
        """
        if isinstance(value, str):
            return f'"{value}"'
        else:
            return str(value)

    lines: list[str] = [""]
    lines.append(indent("def set_requirements(self):", 1))
    lines.append(indent("self.requirements = {", 2))

    for req in cwl.requirements:
        match req.class_:
            case "InlineJavascriptRequirement":
                # We do not need to handle this requirement
                pass
            case "DockerRequirement":
                # TODO
                print("\t[INFO] DockerRequirement not yet supported")
            case "InitialWorkDirRequirement":
                # TODO
                print("\t[INFO] InitialWorkDirRequirement not yet supported")
            case "EnvVarRequirement":
                lines.append(indent('"EnvVarRequirement": {', 3))
                for var in req.envDef:
                    lines.append(indent(f'"{var.envName}": "{var.envValue}",', 4))
                lines.append(indent("},", 3))
            case "InplaceUpdateRequirement":
                lines.append(indent(f'"InplaceUpdateRequirement": {req.inplaceUpdate},', 3))
            case "ResourceRequirement":
                lines.append(indent('"ResourceRequirement": {', 3))
                if hasattr(req, "coresMax") and req.coresMax is not None:
                    lines.append(indent(f'"coresMax": {quote(req.coresMax)},', 4))
                if hasattr(req, "coresMin") and req.coresMin is not None:
                    lines.append(indent(f'"coresMin": {quote(req.coresMin)},', 4))
                if hasattr(req, "ramMin") and req.ramMin is not None:
                    lines.append(indent(f'"ramMin": {quote(req.ramMin)},', 4))
                if hasattr(req, "ramMax") and req.ramMax is not None:
                    lines.append(indent(f'"ramMax": {quote(req.ramMax)},', 4))
                if hasattr(req, "tmpdirMin") and req.tmpdirMin is not None:
                    lines.append(indent(f'"tmpdirMin": {quote(req.tmpdirMin)},', 4))
                if hasattr(req, "outdirMin") and req.tmpdirMax is not None:
                    lines.append(indent(f'"outdirMin": {quote(req.outdirMin)},', 4))
                lines.append(indent("},", 3))
            case _:
                raise NotImplementedError(f"Found unsupported requirement {req.class_}")

    lines.append(indent("}", 2))

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_io(
        cwl: CommandLineTool,
        out_file: TextIO
    ) -> None:
    """
    Parse stdin, stdout, stderr, and exit codes and write to PWF file.
    TODO Test
    """

    lines: list[str] = [""]
    lines.append(indent("def set_io(self):", 1))
    lines.append(indent("self.io = {", 2))
    if hasattr(cwl, "stdin") and cwl.stdin is not None:
        lines.append(indent(f'"stdin": "{normalize(cwl.stdin)}",', 3))
    if hasattr(cwl, "stdout") and cwl.stdout is not None:
        lines.append(indent(f'"stdout": "{normalize(cwl.stdout)}",', 3))
    if hasattr(cwl, "stderr") and cwl.stderr is not None:
        lines.append(indent(f'"stderr": "{normalize(cwl.stderr)}",', 3))
    if hasattr(cwl, "successCodes") and cwl.successCodes is not None:
        lines.append(indent(f'"successCodes": {cwl.successCodes},', 3))
    if hasattr(cwl, "temporaryFailCodes") and cwl.temporaryFailCodes is not None:
        lines.append(indent(f'"temporaryFailCodes": {cwl.temporaryFailCodes},', 3))
    if hasattr(cwl, "permanentFailCodes") and cwl.permanentFailCodes is not None:
        lines.append(indent(f'"permanentFailCodes": {cwl.permanentFailCodes},', 3))
    lines.append(indent("}", 2))

    if len(lines) == 4:
        # Special case: no stdin, stdout, stderr, successCodes, 
        # temporaryFailCodes, permanentFailCodes
        lines.clear()
        lines.append("")
        lines.append(indent("def set_io(self):", 1))
        lines.append(indent("self.io = {}", 2))

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_steps(
        cwl: Workflow,
        out_file: TextIO
    ) -> None:
    """
    Parse workflow steps and write to file.
    """
    def resolve_source(source: str) -> str:
        return "/".join(source.split("#")[-1].split("/")[1:])
    
    def resolve_run_uri(run_script_uri: str, step_id: str) -> str:
        run_script_path = Path(run_script_uri[7:])      # Remove 'file://'
        run_script_filename = run_script_path.name[:-4] + ".py" # Extract filename
        file_path = Path(step_id[7:].split("#")[0])
        rel_path = Path(os.path.relpath(run_script_path.parent, file_path.parent))
        return f"{rel_path / run_script_filename}"


    lines: list[str] = [""]
    lines.append(indent("def set_steps(self):", 1))
    lines.append(indent("self.steps = {", 2))


    # Make sure we are dealing with a list of steps
    steps: List = []
    if isinstance(cwl.steps, List):
        steps = cwl.steps
    elif isinstance(cwl.steps, Mapping):
        # Output parameters are saved in a dict and must be transformed to
        # a list by including the output paramater ID as a key in the 
        # output parameter. 
        for id, output_dict in cwl.steps:
            output_dict["id"] = id
            steps.append(output_dict)
    else:
        raise Exception(f"Expected list/dict, but found '{type(cwl.inputs)}'")

    for step in steps:
        # ID
        # NOTE Force id field in cwl file?
        lines.append(indent(f'"{step.id.split("/")[-1]}": {{', 3))

        # in
        lines.append(indent(f'"in": {{', 4))
        for i in step.in_:
            lines.append(indent(f'"{i.id.split("/")[-1]}": {{', 5))
            if hasattr(i, "source") and i.source is not None:
                if (isinstance(i.source, str)):
                    # Single source
                    lines.append(indent(f'"source": "{resolve_source(i.source)}",', 6))
                else:   
                    # array
                    if len(i.source) == 1:
                        # Single source
                        lines.append(indent(f'"source": "{resolve_source(i.source[0])}",', 6))
                    else:
                        # Multiple sources
                        lines.append(indent(f'"source": [', 6))
                        for s in i.source:
                            lines.append(indent(f'"{resolve_source(s)}",', 7))
                        lines.append(indent(f'],', 6))
                        print("\t[INFO] Step input with multiple sources not yet supported")

            if hasattr(i, "default") and i.default is not None:
                lines.append(indent(f'"default": "{i.default}",', 6))
            if hasattr(i, "valueFrom") and i.valueFrom is not None:
                lines.append(indent(f'"valueFrom": "{i.valueFrom}"', 6))
            lines.append(indent('},', 5))
        lines.append(indent('},', 4))

        # out
        lines.append(indent(f'"out": [', 4))
        for out in step.out:
            lines.append(indent(f'"{out.id.split("/")[-1]}",', 5))
        lines.append(indent('],', 4))
            
        # run
        run_uri: str = resolve_run_uri(step.run, step.id)
        lines.append(indent(f'"run": "{run_uri}",', 4))

        # Label
        if hasattr(step, "label") and step.label is not None:
            lines.extend(format_dict_key_string("label", step.label, 4))

        # Doc
        if hasattr(step, "doc") and step.doc is not None:
            lines.extend(format_dict_key_string("doc", step.doc, 4))

        lines.append(indent("},", 3))
    lines.append(indent("}", 2))

    # Add newlines to each string and write to file
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_workflow_requirements(
        cwl: Workflow,
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = [""]

    # TODO: implement workflow requirements parsing

    # Add newlines to each string and write to file
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_suffix(
        class_name: str,
        out_file: TextIO
    ) -> None:
    """
    Add the execution block to the end of the file.
    """
    lines: list[str] = [""]
    lines.append('if __name__ == "__main__":')

    # Add newlines to each string and write to file
    lines = [line + "\n" for line in lines]
    lines.append(f"\t{class_name}()")
    out_file.writelines(lines)


def parse_cwl(
        cwl: Process, 
        output_path: Path
    ) -> None:
    """
    Parse a CWL Process object and write to PWF output file.
    Note: ExpressionTool is not supported.
    """
    # Generate class name from output file stem + _PWF suffix.
    # This is to avoid name clashes within the Python environment.
    class_name = str(output_path.stem) + "_PWF"

    # Expression tools are extracted as normal tools
    if isinstance(cwl, ExpressionTool):
        raise NotImplementedError("ExpressionTool transpilation is not supported")

    with open(output_path, "w") as f:
        parse_prefix(cwl, class_name, f)
        parse_metadata(cwl, f)
        parse_inputs(cwl, f)
        parse_outputs(cwl, f)
        # if "CommandLineTool" in cwl.class_:
        if isinstance(cwl, CommandLineTool):
            parse_base_command(cwl, f)
            parse_tool_requirements(cwl, f)
            parse_io(cwl, f)
        # elif "Workflow" in cwl.class_:
        elif isinstance(cwl, Workflow):
            parse_steps(cwl, f)
            parse_workflow_requirements(cwl, f)
        parse_suffix(class_name, f)


def transpile_file(
        cwl_file_path: Path,
        output_file_path: Path
    ) -> None:
    """
    Transpile a single CWL file to a PWF file.
    """
    # Create path for the CWL input file
    # if isinstance(cwl_file_path, str):
    # cwl_file_path: Path = Path(cwl_file_path_string)
    # elif not isinstance(cwl_file_path, Path):
        # raise TypeError(f"Expected 'str' or 'Path', but got {type(cwl_file_path)}")
    
    # Get path for the PWF output file
    # if output_file_path is not None:
    #     # if isinstance(output_file_path, str):
    #     output_file_path = Path(output_file_path_string)
    #     # elif not isinstance(output_file_path, Path):
    #         # raise TypeError(f"Expected 'str' or 'Path', but got {type(output_file_path)}")
    # else:
    #     # Use cwl_file_path basename for the output file
    #     basename = str(cwl_file_path.stem)
    #     output_file_path = Path(basename + ".py")

    cwl_dict = load_document_by_uri(cwl_file_path)
    parse_cwl(cwl_dict, output_file_path)



def transpile_files(
        cwl_file_paths: list[Path],
        output_file_paths: list[Path]
    ) -> None:
    """
    Transpile multiple CWL files to PWF files.
    """
    for in_path, out_path in zip(cwl_file_paths, output_file_paths):
        print(f"Transpiling '{in_path}' to '{out_path}'")
        transpile_file(in_path, out_path)
        

def create_parser() -> argparse.ArgumentParser:
    """
    Create and return the argument parser for the transpiler CLI.

    -i, --input INPUT [INPUT ...]
        One or more CWL files to transpile. (required)

    Usage schema (mutually exclusive, -d is default if none given):
      -o OUT [OUT ...]  Specify one or more explicit output paths (one per input cwl file).
      -s                Write outputs to the same directory as their respective input files.
      -d DIRECTORY      Write outputs into the given DIRECTORY for all inputs.

    Returns:
        Configured argparse.ArgumentParser
    """

    arg_parser = argparse.ArgumentParser(
        prog="progname",
        description=""
    )
    arg_parser.add_argument(
        "-i", "--input",
        required=True,
        nargs="+",
        help="One or more CWL files to transpile."
    )

    # Optional mutually exclusive schema: -o [str1 str2 ...] | -c | -d str
    group = arg_parser.add_argument_group(
        title = "Output options",
        description = "Specify one of the output options below. If None is given, '-d .' is used by default."
    )
    subgroup = group.add_mutually_exclusive_group()
    subgroup.add_argument(
        "-o", "--output",
        nargs="+",
        help="One or more output paths (one per input when multiple inputs.)",
        metavar="OUT"
    )
    subgroup.add_argument(
        "-d", "--directory",
        type=str,
        metavar="DIRECTORY",
        help="Write all outputs into the given directory.",
        default="."
    )
    subgroup.add_argument(
        "-s", 
        action="store_true",
        help="Write the output of each input file to the same directory as the input file."
    )
    return arg_parser


def main():
    arg_parser = create_parser()
    args = arg_parser.parse_args()

    input_paths = []
    output_paths = []

    # Check if all input paths are valid.
    # Wildcard paths are supported by using glob.
    for input_path in args.input:
        input_path_obj = Path(input_path)
        input_paths.extend(input_path_obj.parent.glob(input_path_obj.name))

    # Determine output paths based on specified method
    if args.s:
        # Output to same directory as the respective input files
        for input_path in input_paths:
            input_path_obj = Path(input_path)
            output_paths.append(input_path_obj.parent / (input_path_obj.stem + ".py"))
    elif args.output:
        # Output paths given directly
        if len(args.input) != len(args.output):
            raise Exception(f"Input and output lengths don't match ({len(args.input)} != {len(args.output)})")
        output_paths = args.output
    elif args.directory:
        # Output directory given, generate output paths for each input.
        # If no other method was specified, this is the default.
        output_dir = Path(args.directory)
        output_dir.mkdir(parents=True, exist_ok=True)
        for input_path in input_paths:
            input_path_obj = Path(input_path)
            output_paths.append(output_dir / (input_path_obj.stem + ".py"))
    else:
        # Should be guarded against by mutually exclusive group with -d default
        raise Exception("No output method specified")

    transpile_files(input_paths, output_paths)


if __name__ == "__main__":
    main()