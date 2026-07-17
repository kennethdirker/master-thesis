# General imports that are always needed
import dask, subprocess, sys, yaml
from dask.distributed import Client

# File dependent imports
from utils import FileObject, glob, js_eval

@dask.delayed
def noiseremover(input_obj: dict, context: dict) -> dict:
    """
    class: CommandLineTool
    label: noiseremover
    """
    def outputs_output(context: dict):
        pattern = js_eval("inputs.output_file_name", context)
        return FileObject(glob(pattern)[0])

    # Convert values from YAML to correct types  
    inputs = {
        "input": None, 
        "output_file_name": None
    }
    inputs.update(input_obj)
    tool_context = {"inputs": inputs, **context}

    # Build the command
    cmd = [
        "python",
        "noiseremover.py",
        str(inputs["input"]),
        inputs["output_file_name"]
    ]
    print("Running:", *cmd)
    subprocess.run(cmd)

    return {
        "output": outputs_output(tool_context)
    }


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    env = {}

    # Submit to Dask
    result = client.compute(noiseremover(input_yaml, env)).result()
    print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
    main()