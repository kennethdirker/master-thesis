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
id: imageplotter
label: imageplotter
"""

def exec_cmd(cmd: list[str], env: dict) -> dict:
    outputs = {}
    subprocess.run(cmd)
    outputs["output"] = glob.glob(env["inputs"]["output_image"])
    return outputs


def download_images(input_yaml: dict) -> tuple[list[str], dict]:
    cmd: list[str] = []

    env = {}

    # Convert values from YAML to correct types  
    inputs = {}
    inputs["url_list"] = [FileObject(f) for f in input_yaml["url_list"]]
    inputs["output_image"] = str("output_image")


    # Build the command
    cmd = [
        "python",
        "imageplotter.py",
        *[str(s) for s in inputs["url_list"]],
        inputs["output_image"]
    ]
    
    return cmd, env


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    env = {}
    cmd, env = download_images(input_yaml, env)
    # Submit to Dask
    future = client.submit(exec_cmd, cmd)
    print(future.result())


if __name__ == "__main__":
    main()