# General imports that are always needed
import dask, subprocess, sys, yaml
from dask.distributed import Client

# File dependent imports
from utils import FileObject, glob, js_eval

@dask.delayed
def imageplotter(input_obj: dict, context: dict) -> dict:
    """
    class: CommandLineTool
    label: imageplotter
    """
    def outputs_output(context: dict):
        pattern = js_eval("inputs.output_image", context)
        return FileObject(glob(pattern)[0])

    # Set input defaults and bind values from the input object 
    inputs = {"input_fits": None, "output_image": None}
    inputs.update(input_obj)
    tool_context = {"inputs": inputs, **context}

    # Build the command
    cmd = [
        'python',
        'imageplotter.py',
        *[str(s) for s in inputs['input_fits']],
        inputs['output_image']
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

    # Run as task graph with Dask delayed
    result = client.compute(imageplotter(input_yaml, env)).result()
    print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
    main()