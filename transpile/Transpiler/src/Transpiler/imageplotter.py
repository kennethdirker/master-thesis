import dask, subprocess, sys, yaml
from dask.distributed import Client
from dask_jobqueue.slurm import SLURMCluster
from utils import FileObject, glob, js_eval

@dask.delayed
def imageplotter(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	label: imageplotter
	"""
	def outputs_output(context):
		pattern = js_eval("inputs.output_image", context)
		return glob(pattern)

	# Gather inputs in their correct format
	inputs = {
		"input_fits": None,
		"output_image": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Ready the commandline and execute the tool
	cmd = [
		'python',
		'imageplotter.py',
		str(inputs["input_fits"]),
		str(inputs["output_image"]),
	]
	print("Running:",  *cmd)
	subprocess.run(cmd)

	# Collect and generate outputs
	outputs: dict = {}
	outputs["output"] = FileObject(outputs_output(tool_context))
	return outputs

def main():
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
	client = Client(cluster)

	# Convert input YAML to dict
	with open(sys.argv[1], "r") as f:
		input_yaml = yaml.load(f, Loader=yaml.BaseLoader)

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(imageplotter(input_yaml, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
