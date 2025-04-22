import argparse

from pathlib import Path
from typing import Any, Optional, TextIO, Union

from cwl_utils.parser import load_document_by_uri



def parse_prefix(
        cwl: dict[str, Any],
        class_name: str,
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = []

    if "CommandLineTool" in cwl.class_:
        lines.append("from PWF.src.CommandLineTool import BaseCommandLineTool")
        lines.append("")
        lines.append(f"class {class_name}(BaseCommandLineTool):")
    elif "Workflow" in cwl.class_:
        lines.append("from PWF.src.Workflow import BaseWorkflow")
        lines.append("")
        lines.append(f"class {class_name}(BaseWorkflow):")
    lines.append("")
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_metadata(
        cwl: dict[str, Any],
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = ["\tdef set_metadata(self):"]
    # Label
    if hasattr(cwl, "label") and cwl.label is not None:
        lines.append(f'\t\tself.label = "{cwl.label}"')

    # Doc
    if hasattr(cwl, "doc") and cwl.doc is not None:
        doc_len = len(cwl.doc)
        lines: list[str] = ["\t\tself.doc = ("]
        begin = 0
        while begin < doc_len:
            end = min(begin + 60, doc_len)
            lines.append(f'\t\t\t"{cwl.doc[begin:end]}"')
            begin += 60
        lines.append("\t\t)")

    # Insert 'pass' when the file has no label or doc
    if len(lines) == 1:
        lines.append("\t\tpass")

    # Add newlines to each string
    lines = [line + "\n" for line in lines]
    out_file.writelines(lines)


def parse_inputs(
        cwl: dict[str, Any],
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = []
    if hasattr(cwl, "inputs"):
        print(cwl.inputs)
        # for input_id, input_dict in cwl["inputs"].items():
            
    raise NotImplementedError()


def parse_outputs(
        cwl: dict[str, Any],
        out_file: TextIO
    ) -> None:
    """
    
    """
    raise NotImplementedError()


def parse_base_command(
        cwl: dict[str, Any],
        out_file: TextIO
    ) -> None:
    """
    
    """
    raise NotImplementedError()


def parse_steps(
        cwl: dict[str, Any],
        out_file: TextIO
    ) -> None:
    """
    
    """
    raise NotImplementedError()


def parse_suffix(
        class_name: str,
        out_file: TextIO
    ) -> None:
    """
    
    """
    lines: list[str] = ['if __name__ == "__main__":']
    lines = [line + "\n" for line in lines]
    lines.append(f"\t{class_name}(main=True)")


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
        if "CommandLineTool" in cwl["class_"]:
            parse_base_command(cwl, f)
        elif "Workflow" in cwl["class_"]:
            parse_steps(cwl, f)
        parse_suffix(cwl, class_name, f)


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
        basename = str(Path.stem)
        output_file_path = Path(basename + ".py")

    cwl_dict = load_document_by_uri(cwl_file_path)
    parse_cwl(cwl_dict, output_file_path)



def transpile_files(
        cwl_file_paths: list[Union[str, Path]],
        output_file_paths: Optional[list[Union[str, Path]]]
    ) -> None:
    """
    
    """
    raise NotImplementedError()
    pass


def main():
    arg_parser = argparse.ArgumentParser(
        prog="progname",
        description=""
    )
    arg_parser.add_argument("-i", "--input", nargs="+")#, type=argparse.FileType("r"))
    arg_parser.add_argument("-o", "--output", nargs="*")#, type=argparse.FileType("w"))
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