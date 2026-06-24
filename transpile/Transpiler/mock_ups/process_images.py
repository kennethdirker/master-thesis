# General imports that are always needed
import dask, subprocess, sys, yaml
from dask.distributed import Client

# File dependent imports
import glob
from utils import FileObject, js_eval

# # Typing imports
# from typing import Any

@dask.delayed
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


@dask.delayed
def noiseremover(input_obj: dict, env: dict) -> dict:
    """
    class: CommandLineTool
    id: noiseremover
    label: noiseremover
    """
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

@dask.delayed
def process_images(input_obj: dict, env: dict) -> dict:
    """
    class: Workflow
    id: process_images
    label: process_images
    """
    def noiseremover_input_valueFrom():
        ...
    def noiseremover_output_file_name_valueFrom():
        ...

    outputs = {}
    inputs = {}
    inputs["fit_list"] = [FileObject(f) for f in input_obj["fit_list"]]

    imageplotter_inputs = {
        "input_fits": inputs["fit_list"],
        "output_image": "before_noise_remover.png"
    }
    imageplotter_out = imageplotter(imageplotter_inputs, env)

    noiseremover_inputs = {
        "input": noiseremover_input_valueFrom(),
        
        "output_file_name": noiseremover_output_file_name_valueFrom()
    }
    noiseremover_out = noiseremover(noiseremover_inputs, env)
    after_plot_inspect_out = imageplotter(after_plot_inspect_inputs, env)


    outputs[""] = ...
    return outputs


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    env = {}

    # Submit to Dask
    # future = client.submit(process_images, input_yaml, env)
    # print(future.result())
    t = process_images(input_yaml, env)
    print(client.compute(t))


if __name__ == "__main__":
    main()