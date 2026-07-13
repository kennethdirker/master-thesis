import dask, subprocess, sys, yaml
from dask.distributed import Client
from dask_jobqueue.slurm import SLURMCluster
from utils import FileObject, glob, js_eval

@dask.delayed
def noiseremover(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	label: noiseremover
	"""
	def outputs_output(context):
		pattern = js_eval("inputs.output_file_name", context)
		return FileObject(glob(pattern)[0])

	# Gather inputs in their correct format
	inputs = {
		"input": None,
		"output_file_name": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Ready the commandline and execute the tool
	cmd = [
		'python',
		'noiseremover.py',
		str(inputs["input"]),
		str(inputs["output_file_name"]),
	]
	print("Running:",  *cmd)
	subprocess.run(cmd)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


@dask.delayed
def imageplotter(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	label: imageplotter
	"""
	def outputs_output(context):
		pattern = js_eval("inputs.output_image", context)
		return FileObject(glob(pattern)[0])

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
		*[str(v) for v in inputs["input_fits"]],
		str(inputs["output_image"]),
	]
	print("Running:",  *cmd)
	subprocess.run(cmd)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


@dask.delayed
def process_images(input_obj: dict, context: dict) -> dict:
	"""
	class: Workflow
	label: process_images
	"""

	# Gather inputs in their correct format
	inputs = {
		"fit_list": None,
	}
	inputs.update(input_obj)

	# Step ID:    imageplotter
	# Step label: imageplotter
	imageplotter_in = {
		"input_fits": inputs["fit_list"],
		"output_image": "before_noise_remover.png",
	}

	# Step ID:    noiseremover
	# Step label: noiseremover
	noiseremover_in = {
		"input": inputs["fit_list"],
	}

	# Step ID:    after_plot_inspect
	# Step label: imageplotter
	after_plot_inspect_in = {
		"input_fits": noiseremover_out["output"],
		"output_image": "after_noise_remover.png",
	}

	# Compute outputs



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
	result = client.compute(process_images(input_yaml, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
