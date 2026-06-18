# General imports that are always needed
import subprocess, sys, yaml
from dask.distributed import Client

# Typing imports
from typing import Any

# File dependent imports
import glob
# from utils import convert2type   # pyright: ignore[reportMissingImports]
def convert2type(t: str, value: Any):
    if "File" in t:
        if isinstance(value, str):
            return value
        elif isinstance(value, dict):
            return value["path"]

"""
class: CommandLineTool
id: download_images
label: download_images
"""

def exec_cmd(cmd: list[str]) -> dict:
    outputs = {}
    subprocess.run(cmd)
    outputs["output"] = glob.glob("*.fits")
    return outputs


def download_images(input_yaml: dict) -> list[str]:
    cmd: list[str] = []
    # Convert values from YAML to correct types  
    inputs = {}
    inputs["url_list"] = convert2type("File", input_yaml["url_list"])

    # Build the command
    cmd = ["wget", "-i", str(inputs["url_list"])]
    return cmd


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    # Submit to Dask
    future = client.submit(exec_cmd, download_images(input_yaml))
    print(future.result())


if __name__ == "__main__":
    main()