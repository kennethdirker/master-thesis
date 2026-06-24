# General imports that are always needed
import subprocess, sys, yaml
from dask.distributed import Client

# Typing imports
from typing import Any

# File dependent imports
import glob
from utils import FileObject

def download_images(input_obj: dict) -> dict:
    """
    class: CommandLineTool
    id: download_images
    label: download_images
    """
    cmd: list[str] = []
    outputs = {}

    # Convert values from YAML to correct types  
    inputs = {}
    inputs["url_list"] = FileObject(input_obj["url_list"])

    # Build the command
    cmd = [
        "wget",
        "-i",
        str(inputs["url_list"])
    ]

    subprocess.run(cmd)
    outputs["output"] = glob.glob("*.fits")
    return outputs


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    # Submit to Dask
    future = client.submit(download_images, input_yaml)
    print(future.result())


if __name__ == "__main__":
    main()