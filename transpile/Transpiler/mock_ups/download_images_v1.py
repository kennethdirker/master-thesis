# General imports that are always needed
import subprocess, sys, yaml
from dask.distributed import Client

# Typing imports
from typing import Any

# File dependent imports
import glob
from utils import FileObject

"""
class: CommandLineTool
id: download_images
label: download_images
"""

def exec_cmd(cmd: list[str]):
    outputs = {}
    subprocess.run(cmd)
    outputs["output"] = glob.glob("*.fits")
    return outputs


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    # Convert values from YAML inputs to correct types  
    inputs: dict[Any] = {}
    inputs["url_list"] = FileObject(input_yaml["url_list"])


    # Build the command
    cmd = [
        "wget",
        "-i",
        str(inputs["url_list"])
    ]

    # Submit to Dask
    future = client.submit(exec_cmd, cmd)
    print(future.result())


if __name__ == "__main__":
    main()