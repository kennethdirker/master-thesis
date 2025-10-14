import argparse
import os

from pathlib import Path
from typing import Any, Optional, TextIO, Union

from cwl_utils.parser import load_document_by_uri

from cwl_utils.parser.cwl_v1_2 import (
    CommandOutputArraySchema, 
    CommandInputArraySchema,
    WorkflowStepOutput
)

def indent(str, n) -> str:
    return "\t" * n + str

def parse_prefix(
        cwl: dict[str, Any],
        class_name: str,
        out_file: TextIO
    ) -> None:
    """
    
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
        cwl,
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = [""]
    lines.append(indent("def set_metadata(self):", 1))

    # Label
    if hasattr(cwl, "label") and cwl.label is not None:
        lines.append(indent(f'self.label = "{cwl.label}"', 2))

    # Doc
    if hasattr(cwl, "doc") and cwl.doc is not None:
        doc_len = len(cwl.doc)
        lines.append(indent("self.doc = (", 2))
        begin = 0
        while begin < doc_len:
            end = min(begin + 60, doc_len)
            lines.append(indent(f'"{cwl.doc[begin:end]}"', 3))
            begin += 60
        lines.append(indent(")", 2))

    # Insert 'pass' when the file has no label or doc
    if len(lines) == 1:
        lines.append(indent("pass", 2))

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def get_input_type(type_: Any) -> list[str]:
    """
    
    """
    lines: list[str] = []

    if isinstance(type_, CommandInputArraySchema):
        lines.append(indent(f'"type": "{type_.items.lower()}[]",', 4))
    elif isinstance(type_, str):
        lines.append(indent(f'"type": "{type_.lower()}",', 4))
    else:
        raise NotImplementedError(f"Found unsupported type {type(type_)}")

    return lines


def parse_inputs(
        cwl,
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = [""]
    lines.append(indent("def set_inputs(self):", 1))
    lines.append(indent("self.inputs = {", 2))
    
    if hasattr(cwl, "inputs"):
        for input in cwl.inputs:
            # ID
            lines.append(indent(f'"{input.id.split("/")[-1]}": {{', 3))

            # Type
            type_: list[str] = get_input_type(input.type_)
            lines.extend(type_)
            
            # Inputbinding
            if hasattr(input, "inputBinding"):
                # Prefix
                binding = input.inputBinding
                if hasattr(binding, "prefix") and binding.prefix is not None:
                    lines.append(indent(f'"prefix": "{binding.prefix}",', 4))
                # Position
                if hasattr(binding, "position") and binding.position is not None:
                    lines.append(indent(f'"position": {binding.position},', 4))
            lines.append(indent("},", 3))
    lines.append(indent("}", 2))

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def get_output_type(type_: Any) -> list[str]:
    """
    
    """
    lines: list[str] = []

    if isinstance(type_, CommandOutputArraySchema):
        lines.append(indent(f'"type": "{type_.items.lower()}[]",', 4))
    elif isinstance(type_, str):
        lines.append(indent(f'"type": "{type_.lower()}",', 4))
    else:
        raise NotImplementedError(f"Found unsuppored type {type(type_)}")

    return lines


def get_glob(glob: str) -> str:
    """
    
    """
    s = glob
    if "$(" in glob and ")" in glob:
        s = glob[2:-1].split(".")
        if "inputs" in s[0]:
            # Get glob from input
            glob = s[1]
        else:
            # Get glob from step
            glob = "/".join(s[1:])
        s = f"${glob}$"
    return s


def parse_outputs(
        cwl,
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = [""]
    lines.append(indent("def set_outputs(self):", 1))
    lines.append(indent("self.outputs = {", 2))
    
    if hasattr(cwl, "outputs"):
        for output in cwl.outputs:
            # ID
            lines.append(indent(f'"{output.id.split("/")[-1]}": {{', 3))

            # Type
            type_: list[str] = get_output_type(output.type_)
            lines.extend(type_)
            
            # Glob
            if hasattr(output, "outputBinding"):
                binding = output.outputBinding
                if hasattr(binding, "glob"):
                    glob = get_glob(binding.glob)
                    lines.append(indent(f'"glob": "{glob}",', 4))

            lines.append(indent("},", 3))
    lines.append(indent("}", 2))

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_base_command(
        cwl,
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


def resolve_source(source: str) -> str:
    return "/".join(source.split("#")[-1].split("/")[1:])


def resolve_run_uri(run_script_uri: str, step_id: str) -> str:
    run_script_path = Path(run_script_uri[7:])      # Remove 'file://'
    run_script_filename = run_script_path.name[:-4] + ".py" # Extract filename
    
    file_path = Path(step_id[7:].split("#")[0])
    rel_path = Path(os.path.relpath(run_script_path.parent, file_path.parent))
    return f"{rel_path / run_script_filename}"

def resolve_valueFrom(valueFrom: str) -> str:
    return "$" + valueFrom[2:-1] + "$"


def parse_steps(
        cwl,
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = [""]
    lines.append(indent("def set_steps(self):", 1))
    lines.append(indent("self.steps = {", 2))

    for step in cwl.steps:
        # ID
        lines.append(indent(f'"{step.id.split("/")[-1]}": {{', 3))

        # in
        lines.append(indent(f'"in": {{', 4))
        for i in step.in_:
            lines.append(indent(f'"{i.id.split("/")[-1]}": {{', 5))
            if hasattr(i, "source") and i.source is not None:
                if isinstance(i.source, str):
                    lines.append(indent(f'"source": "{resolve_source(i.source)}",', 6))
                else:   # array
                    lines.append(indent(f'"source": [', 6))
                    for s in i.source:
                        lines.append(indent(f'"{resolve_source(s)}",', 7))
                    lines.append(indent(f'],', 6))

            if hasattr(i, "default") and i.default is not None:
                lines.append(indent(f'"default": "{i.default}",', 6))
            if hasattr(i, "valueFrom") and i.valueFrom is not None:
                valueFrom = resolve_valueFrom(i.valueFrom)
                lines.append(indent(f'"valueFrom": "{valueFrom}"', 6))
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

        # label
        if hasattr(step, "label"):
            lines.append(indent(f'"label": "{step.label}",', 4))
        lines.append(indent("},", 3))
    lines.append(indent("}", 2))

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_suffix(
        class_name: str,
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = [""]
    lines.append('if __name__ == "__main__":')
    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    lines.append(f"\t{class_name}()")
    # lines.append(f"\t{class_name}(main=True)")
    out_file.writelines(lines)

def parse_cwl(
        cwl: dict[str, Any], 
        output_path: Path
    ) -> None:
    """
    
    """
    class_name = str(output_path.stem)
    with open(output_path, "w") as f:
        parse_prefix(cwl, class_name, f)
        parse_metadata(cwl, f)
        parse_inputs(cwl, f)
        parse_outputs(cwl, f)
        if "CommandLineTool" in cwl.class_:
            parse_base_command(cwl, f)
        elif "Workflow" in cwl.class_:
            parse_steps(cwl, f)
        parse_suffix(class_name, f)


def transpile_file(
        cwl_file_path: Union[str, Path],
        output_file_path: Optional[Union[str, Path]] = None
    ) -> None:
    """
    
    """
    # Create path for the CWL input file
    if isinstance(cwl_file_path, str):
        cwl_file_path = Path(cwl_file_path)
    elif not isinstance(cwl_file_path, Path):
        raise TypeError(f"Expected 'str' or 'Path', but got {type(cwl_file_path)}")
    
    # Get path for the PWF output file
    if output_file_path:
        if isinstance(output_file_path, str):
            output_file_path = Path(output_file_path)
        elif not isinstance(output_file_path, Path):
            raise TypeError(f"Expected 'str' or 'Path', but got {type(output_file_path)}")
    else:
        # Use cwl_file_path basename for the output file
        basename = str(cwl_file_path.stem)
        output_file_path = Path(basename + ".py")

    cwl_dict = load_document_by_uri(cwl_file_path)
    parse_cwl(cwl_dict, output_file_path)



def transpile_files(
        cwl_file_paths: list[Union[str, Path]],
        output_file_paths: Optional[list[Union[str, Path]]] = None
    ) -> None:
    """
    
    """
    if output_file_paths is None:
        output_file_paths = [None] * len(cwl_file_paths)
    for in_path, out_path in zip(cwl_file_paths, output_file_paths):
        transpile_file(in_path, out_path)
        


def main():
    arg_parser = argparse.ArgumentParser(
        prog="progname",
        description=""
    )
    arg_parser.add_argument("input", nargs="+")
    arg_parser.add_argument("-o", "--output", nargs="*")
    args = arg_parser.parse_args()


    in_len: int = len(args.input)
    if len(args.input) > 1:
        # Multiple CWL files to transpile
        if args.output and isinstance(args.output, list):
            if in_len != len(args.output):
                raise Exception(f"Input and output files not equal ({in_len} != {len(args.output)})")
        transpile_files(args.input, args.output)
    else:
        # Single CWL file to transpile
        output = args.output
        if args.output:
            if len(args.output) > 1:
                raise Exception(f"Multiple output files were given for a single input file")
            output = output[0]
        transpile_file(args.input[0], output)


if __name__ == "__main__":
    main()