# import cwltool
from pathlib import Path
import yaml, argparse


import cwltool.factory

from dask_executor import DaskExecutor

def main(
        # TODO remove defaults
        cwl_process_path: str = "echo_wf.cwl",
        job_order_path: str = "echo_inputs.yaml",
        default: bool = False
):
    prog: str = cwl_process_path
    job_uri: str = job_order_path 
    with open(job_uri) as f:
        if default:
            fac = cwltool.factory.Factory()
        else:
            executor: DaskExecutor = DaskExecutor()
            fac = cwltool.factory.Factory(executor=executor)

        prog: cwltool.factory.Callable = fac.make(prog)
        job: dict = yaml.full_load(f)
        res = prog(**job)
        print("Output:\n", res)
        return res
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog="CWL Dask Tool",
                    description="Execute CWL processes with Dask.",
                    epilog="")
    parser.add_argument("-p", "--process", required=False)
    parser.add_argument("-j", "--job", required=False)
    parser.add_argument("-d", "--default", required=False, default=False)
    args = parser.parse_args()

    kwargs = {}
    if args.process and args.job:
        kwargs["process"] = args.process
        kwargs["job"] = args.job
    else:
        if args.process:
            print("Process path given, but job path is missing")
        elif args.job:
            print("Job path given, but process path is missing")
        print("Continuing by running default test setup...\n")

    kwargs["default"] = args.default

    main(**kwargs)