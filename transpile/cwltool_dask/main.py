# import cwltool
from pathlib import Path
import cwltool.factory
import yaml
from dask_executor import DaskExecutor

prog: str = "echo.cwl"
job_uri: str | Path = Path("echo_inputs.yaml")
with open(job_uri) as f:
    job: dict = yaml.full_load(f)
    executor: DaskExecutor = DaskExecutor()
    fac = cwltool.factory.Factory(executor=executor)
    prog: cwltool.factory.Callable = fac.make(prog)
    res = prog(**job)