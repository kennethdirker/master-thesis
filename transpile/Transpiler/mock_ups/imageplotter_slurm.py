# General imports that are always needed
import dask, subprocess, sys, yaml
from dask.distributed import Client
from dask_jobqueue import SLURMCluster

# File dependent imports
import glob
from utils import FileObject, js_eval


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
    cluster.scale(1)
    # cluster.adapt(minimum_jobs=1, maximum_jobs=4)
    return Client(cluster)


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
    #stdout = subprocess.run(cmd, capture_output=True, text=True).stdout

    outputs["output"] = FileObject(glob.glob(outputs_output_glob())[0])
    return outputs


def main():
    client = get_client()
    # client = Client()

    # Convert input YAML to dict
    with open(sys.argv[1], "r") as f:
        input_yaml = yaml.load(f, Loader=yaml.BaseLoader)
 
    env = {}

    # Run as task graph with Dask delayed
    result = client.compute(imageplotter(input_yaml, env)).result()
    print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
    main()