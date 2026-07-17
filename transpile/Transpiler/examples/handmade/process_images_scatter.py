# General imports that are always needed
import dask, subprocess, sys
from dask.distributed import Client
from dask_jobqueue.slurm import SLURMCluster

# File dependent imports
from utils import FileObject, glob, js_eval, load_input_object, scatterizer, transpose

def get_client() -> Client:
    # Initialize cluster
    # NOTE: Memory argument is forced by the SLURMCluster 
    # initializer. This causes problems on systems that disable
    # setting memory requirements (DAS6 has this restriction). The
    # band-aid is to ignore the memory setting line with
    # 'job_directives_skip'.
    cluster = SLURMCluster(
        cores=16,
        memory="16GB",
        walltime="00:15:00",
        job_directives_skip=['--mem']
    )
    cluster.scale(4)
    # cluster.adapt(minimum_jobs=1, maximum_jobs=4)
    return Client(cluster)


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
    inputs = {
        "input_fits": None,
        "output_image": None,
    }
    inputs.update(input_obj)
    tool_context = {"inputs": inputs, **context}

    # Build the command
    cmd = [
        'python',
        'scripts/imageplotter.py',
        *[str(s) for s in inputs['input_fits']],
        inputs['output_image']
    ]
    print("Running:", *cmd)
    subprocess.run(cmd)

    return {
        "output": outputs_output(tool_context)
    }


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
        "scripts/noiseremover.py",
        str(inputs["input"]),
        inputs["output_file_name"]
    ]
    print("Running:", *cmd)
    subprocess.run(cmd)

    return {
        "output": outputs_output(tool_context)
    }


@dask.delayed
def process_images(input_obj: dict, context: dict) -> dict:
    """
    class: Workflow
    label: process_images
    """
    def noiseremover_output_file_name(context, self):
        context["self"] = self
        return js_eval("'no_noise_' + inputs.input.basename", context)

    inputs = {
        "fit_list": None
    }
    inputs.update(input_obj)
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
    return {
        "before_noise_remover": imageplotter_out["output"].compute(),
        "after_noise_remover_plot": after_plot_inspect_out["output"].compute()
    }


def main():
    # client= get_client()
    client = Client()

    # Load the input object from the YAML file
    input_obj = load_input_object(sys.argv[1])

    context = {}

    # Submit to Dask
    result = client.compute(process_images(input_obj, context)).result()
    print(*[f'{k}: {v}' for k, v in result.items()], sep="\n")

if __name__ == "__main__":
    main()