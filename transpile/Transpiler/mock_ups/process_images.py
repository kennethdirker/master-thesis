# General imports that are always needed
import dask, subprocess, sys, yaml
from dask.distributed import Client

# File dependent imports
from glob import glob
from utils import FileObject, js_eval


# @dask.delayed
def imageplotter(input_obj: dict, context: dict) -> dict:
    """
    class: CommandLineTool
    id: imageplotter
    label: imageplotter
    """
    def outputs_output_glob(context: dict):
        return js_eval("inputs.output_image", context)

    cmd: list[str] = []
    outputs = {}

    # Convert values from YAML to correct types  
    inputs = {}
    inputs["input_fits"] = [FileObject(f) for f in input_obj["input_fits"]]
    inputs["output_image"] = str(input_obj["output_image"])
    local_context = {"inputs": inputs, **context}

    # Build the command
    cmd = [
        "python",
        "scripts/imageplotter.py",
        *[str(s) for s in inputs["input_fits"]],
        inputs["output_image"]
    ]

    subprocess.run(cmd)
    outputs["output"] = FileObject(glob(outputs_output_glob(local_context))[0])
    return outputs


# @dask.delayed
def noiseremover(input_obj: dict, context: dict) -> dict:
    """
    class: CommandLineTool
    id: noiseremover
    label: noiseremover
    """
    def outputs_output_glob(context: dict):
        return js_eval("inputs.output_file_name", context)
    
    cmd: list[str] = []
    outputs = {}

    # Convert values from YAML to correct types  
    inputs = {}
    inputs["input"] = FileObject(input_obj["input"])
    inputs["output_file_name"] = str(input_obj["output_file_name"])
    local_context = {"inputs": inputs, **context}

    # Build the command
    cmd = [
        "python",
        "scripts/noiseremover.py",
        str(inputs["input"]),
        inputs["output_file_name"]
    ]
    
    subprocess.run(cmd)
    outputs["output"] = FileObject(glob(outputs_output_glob(local_context))[0])
    return outputs


# @dask.delayed
def process_images(input_obj: dict, context: dict) -> dict:
    """
    class: Workflow
    id: process_images
    label: process_images
    """
    def noiseremover_input(context, self):
        context["self"] = self
        return js_eval("self[0]", context)
    
    def noiseremover_output_file_name(context, self):
        context["self"] = self
        return js_eval("'no_noise_' + inputs.input[0].basename", context)
    
    def after_plot_inspect(context, self):
        context["self"] = self
        return js_eval("[self]", context)

    outputs = {}
    inputs = {}
    inputs["fit_list"] = [FileObject(f) for f in input_obj["fit_list"]]
    # local_context = {"inputs": inputs, **context} # Only needed when expressions are used outside of steps
    step_context = context.copy()

    # Step ID: imageplotter 
    # Step label: imageplotter 
    imageplotter_inputs = {
        "input_fits": inputs["fit_list"],
        "output_image": "before_noise_remover.png"
    }
    imageplotter_out = imageplotter(imageplotter_inputs, context)

    # Step ID: noiseremover
    # Step label: noiseremover
    noiseremover_inputs = {
        "input": inputs["fit_list"],
    }
    step_context["inputs"] = {**inputs, **noiseremover_inputs}
    noiseremover_inputs["input"] = noiseremover_input(step_context, inputs["fit_list"])
    noiseremover_inputs["output_file_name"] = noiseremover_output_file_name(step_context, None)
    noiseremover_out = noiseremover(noiseremover_inputs, context)

    # Step ID: after_plot_inspect
    # Step label: imageplotter
    after_plot_inspect_inputs = {
        "input_fits": noiseremover_out["output"],
        "output_image": "after_noise_remover.png"
    }
    step_context["inputs"] = {**inputs, **after_plot_inspect_inputs}
    after_plot_inspect_inputs["input_fits"] = after_plot_inspect(step_context, noiseremover_out["output"])
    after_plot_inspect_out = imageplotter(after_plot_inspect_inputs, context)

    # Export outputs
    outputs["before_noise_remover"] = imageplotter_out["output"]
    outputs["after_noise_remover_plot"] = after_plot_inspect_out["output"]
    return outputs


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    context = {}

    # Submit to Dask
    # future = client.compute(process_images(input_yaml, context))
    # print(*[f'{k}: {v}' for k, v in future.result().items()])
    print(*[f'{k}: {v}' for k,v in process_images(input_yaml, context).items()])

if __name__ == "__main__":
    main()