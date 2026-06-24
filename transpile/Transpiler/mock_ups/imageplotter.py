# General imports that are always needed
import dask, subprocess, sys, yaml
from dask.distributed import Client

# File dependent imports
import glob
from utils import FileObject, js_eval

@dask.delayed
def imageplotter(input_obj: dict, env: dict) -> dict:
    """
    class: CommandLineTool
    id: imageplotter
    label: imageplotter
    """
    def outputs_output_glob():
        context = {"inputs": inputs, **env}
        return js_eval("inputs.output_image", context)

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

    subprocess.run(cmd)
    outputs["output"] = FileObject(glob.glob(outputs_output_glob())[0])
    return outputs


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)
 
    env = {}

    # Run as task graph with Dask delayed
    future = client.compute(imageplotter(input_yaml, env))
    print(*[f'{k}: {v}' for k, v in future.result().items()])

if __name__ == "__main__":
    main()