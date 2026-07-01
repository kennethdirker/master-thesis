import dask, subprocess, sys, yaml
from dask.distributed import Client
from dask_jobqueue.slurm import SLURMCluster
from glob import glob
from utils import js_eval

@dask.delayed
def imageplotter(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	label: imageplotter
	"""
	def outputs_output_glob(context: dict):
		return js_eval("$(inputs.output_image)", context)
	inputs = {}
	inputs["input_fits"] = FileObject(input_obj["input_fits"])
	inputs["output_image"] = str(input_obj["output_image"])
	local_context = {"inputs": inputs, **context}
	print("Running:",  *cmd)
	subprocess.run(cmd)

	outputs: dict = {}
	outputs["output"] = FileObject(input_obj["output"])

def main():
	cluster = SLURMCluster(
		cores=16,
		memory="16GB",
		walltime="00:15:00",
		job_directives_skip=['--mem']

	)
	cluster.scale(4)
	client = Client(cluster)

	with open(sys.argv[1], "r") as f:
		input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

	context = {}

	result = client.compute(imageplotter(input_yaml, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
