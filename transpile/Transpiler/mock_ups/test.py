import dask, subprocess, sys
from dask.distributed import Client
from dask_jobqueue import SLURMCluster


def get_client():
    # Add PYTHONPATH to compute-node env
    prologue: list[str] = []
    paths = ":".join(sys.path)
    prologue.append(f"export PYTHONPATH={paths}")

    cluster = SLURMCluster(
        cores=16,
        # processes=6,
        memory="16GB",
        walltime="00:15:00",
        # account="co_laika",
        # queue="savio2_bigmem",
        # job_script_prologue=prologue,
        # python="/var/scratch/kdirker/transpiler/env/bin/python",
        job_directives_skip=['--mem']
    )
    cluster.scale(1) 
    print(cluster.job_script())
    return Client(cluster)

def wrapper():
    return subprocess.run(["hostname"], capture_output=True).stdout

if __name__ == "__main__":
    # client = Client()
    # try:
    client = get_client()
    subprocess.run(["hostname"])
    future = client.submit(wrapper)
    result = future.result()
    print(result)
    # except Exception as e:
        # print(e)
