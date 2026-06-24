# General imports that are always needed
import dask, subprocess, sys, yaml
from dask.distributed import Client

# File dependent imports
import glob
from utils import FileObject, js_eval

@dask.delayed
def noiseremover(input_obj: dict, env: dict) -> dict:
    """
    class: CommandLineTool
    id: noiseremover
    label: noiseremover
    """
    def outputs_output_glob():
        context = {"inputs": inputs, **env}
        return js_eval("inputs.output_file_name", context)
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
    outputs["output"] = FileObject(glob.glob(outputs_output_glob())[0])
    return outputs


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    env = {}

    # Submit to Dask
    future = client.compute(noiseremover(input_yaml, env))
    print(*[f'{k}: {v}' for k, v in future.result().items()])

if __name__ == "__main__":
    main()