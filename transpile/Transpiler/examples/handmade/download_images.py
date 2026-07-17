# General imports that are always needed
import dask, subprocess, sys, yaml
from dask.distributed import Client

# File dependent imports
from utils import FileObject, glob

@dask.delayed
def download_images(input_obj: dict, context: dict) -> dict:
    """
    class: CommandLineTool
    label: download_images
    """
    def outputs_output(context: dict):
        return [FileObject(m) for m in glob("*.fits")]

    # Convert values from YAML to correct types  
    inputs = {
        "url_list": None
    }
    inputs.update(input_obj)
    tool_context = {"inputs": inputs, **context}

    # Build the command
    cmd = [
        "wget",
        "-i", str(inputs["url_list"]),
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
    result = client.compute(download_images(input_yaml, env)).result()
    print(*[f'{k}: {v}' for k, v in result.items()])


if __name__ == "__main__":
    main()