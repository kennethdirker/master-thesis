# General imports that are always needed
import dask, subprocess, sys, yaml
from dask.distributed import Client

# File dependent imports
from glob import glob
from utils import FileObject, js_eval, scatterizer, transpose


@dask.delayed
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
    print("Running:", *cmd)
    subprocess.run(cmd)

    outputs["output"] = FileObject(glob(outputs_output_glob(local_context))[0])
    return outputs


@dask.delayed
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
    print("Running:", *cmd)
    subprocess.run(cmd)

    outputs["output"] = FileObject(glob(outputs_output_glob(local_context))[0])
    return outputs


@dask.delayed
def process_images(input_obj: dict, context: dict) -> dict:
    """
    class: Workflow
    id: process_images
    label: process_images
    """
    
    def noiseremover_output_file_name(context, self):
        context["self"] = self
        return js_eval("'no_noise_' + inputs.input.basename", context)

    outputs = {}
    inputs = {}
    inputs["fit_list"] = [FileObject(f) for f in input_obj["fit_list"]]
    step_context = context.copy()

    # Step ID: imageplotter 
    # Step label: imageplotter 
    imageplotter_in = {
        "input_fits": inputs["fit_list"],
        "output_image": "before_noise_remover.png"
    }
    imageplotter_out = imageplotter(imageplotter_in, context)

    # Step ID: noiseremover
    # Step label: noiseremover
    noiseremover_in = {
        "input": inputs["fit_list"],
    }
    noiseremover_scattered_out = []
    for scattered_inputs in scatterizer(noiseremover_in, "input"):
        step_context["inputs"] = {**inputs, **scattered_inputs}
        scattered_inputs["output_file_name"] = noiseremover_output_file_name(step_context, None)
        noiseremover_scattered_out.append(noiseremover(scattered_inputs, context))
    noiseremover_out = dask.delayed(transpose)(noiseremover_scattered_out)

    # Step ID: after_plot_inspect
    # Step label: imageplotter
    after_plot_inspect_in = {
        "input_fits": noiseremover_out["output"],
        "output_image": "after_noise_remover.png"
    }
    after_plot_inspect_out = imageplotter(after_plot_inspect_in, context)

    # Export outputs
    outputs["before_noise_remover"] = imageplotter_out["output"].compute()
    outputs["after_noise_remover_plot"] = after_plot_inspect_out["output"].compute()
    return outputs


def main():
    client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

    context = {}

    # Submit to Dask
    result = client.compute(process_images(input_yaml, context), retries=0).result()
    print(*[f'{k}: {v}' for k, v in result.items()], sep="\n")

if __name__ == "__main__":
    main()