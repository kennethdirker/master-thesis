# General imports that are always needed
import subprocess, sys, yaml
from dask.distributed import Client

# File dependent imports
import glob
from utils import FileObject

# Typing imports
from typing import Any

"""
class: CommandLineTool
id: noiseremover
label: noiseremover
"""

def noiseremover(input_obj: dict, env: dict) -> dict:
    cmd: list[str] = []
    outputs = {}

    # Convert values from YAML to correct types  
    inputs = {}
    inputs["input"] = FileObject(input_obj["input"])
    inputs["output_file_name"] = str(input_obj["output_file_name"])


    # Build the command
    cmd = [
        "python",
        "scripts/noiseremover.py",
        str(inputs["input"]),
        inputs["output_file_name"]
    ]
    
    subprocess.run(cmd)
    outputs["output"] = FileObject(glob.glob(inputs["output_file_name"])[0])
    return outputs


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    env = {}

    # Submit to Dask
    future = client.submit(noiseremover, input_yaml, env)
    print(future.result())


if __name__ == "__main__":
    main()