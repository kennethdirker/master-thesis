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
id: imageplotter
label: imageplotter
"""

def imageplotter(input_obj: dict, env: dict) -> dict:
    cmd: list[str] = []
    outputs = {}

    # Convert values from YAML to correct types  
    inputs = {}
    inputs["input_fits"] = [FileObject(f) for f in input_obj["input_fits"]]
    inputs["output_image"] = str(input_obj["output_image"])


    # Build the command
    cmd = [
        "python",
        "scripts/imageplotter.py",
        *[str(s) for s in inputs["input_fits"]],
        inputs["output_image"]
    ]

    print(" ".join(cmd))
    
    subprocess.run(cmd)
    outputs["output"] = FileObject(glob.glob(inputs["output_image"])[0])
    return outputs


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    env = {}

    # Submit to Dask
    future = client.submit(imageplotter, input_yaml, env)
    print(future.result())
    # print(imageplotter(input_yaml, env))

if __name__ == "__main__":
    main()