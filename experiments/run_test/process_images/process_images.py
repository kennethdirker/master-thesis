import dask, subprocess
from CWL2DASK.scripting import (
	FileObject,
	checkout,
	finalize,
	glob,
	initial_work_dir_requirement,
	js_eval,
	process_cli_args,
	scatterizer,
	transpose,
)
from dask.distributed import Client

from dask_jobqueue.slurm import SLURMCluster


@dask.delayed
def _noiseremover(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	label: noiseremover
	"""
	# Expression functions: js_eval should be replaced with Python
	def outputs_output(context, cwd):
		pattern = js_eval("inputs.output_file_name", context)
		return FileObject(glob(pattern, cwd)[0])

	# Create a clean temporary working directory and fitted environment dict
	tool_env, cwd = checkout(env)

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Stage files and directories to the temporary working directory
	initial_work_dir_requirement([
		FileObject({
			"class": "File",
			"path": "/var/scratch/kdirker/master-thesis/experiments/run_test/process_images/noiseremover.py",
		}),
	], tool_env)

	# Ready the commandline and execute the tool
	cmd = [
		'/var/scratch/kdirker/master-thesis/experiments/run_test/process_images/env/bin/python3.11',
		'noiseremover.py',
		str(inputs["input"]),
		str(inputs["output_file_name"]),
	]
	print("Running:", *cmd)
	subprocess.run(cmd, env=tool_env, cwd=cwd)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context, cwd),
	}


@dask.delayed
def _imageplotter(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	label: imageplotter
	"""
	# Expression functions: js_eval should be replaced with Python
	def outputs_output(context, cwd):
		pattern = js_eval("inputs.output_image", context)
		return FileObject(glob(pattern, cwd)[0])

	# Create a clean temporary working directory and fitted environment dict
	tool_env, cwd = checkout(env)

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Stage files and directories to the temporary working directory
	initial_work_dir_requirement([
		FileObject({
			"class": "File",
			"path": "/var/scratch/kdirker/master-thesis/experiments/run_test/process_images/imageplotter.py",
		}),
	], tool_env)

	# Ready the commandline and execute the tool
	cmd = [
		'/var/scratch/kdirker/master-thesis/experiments/run_test/process_images/env/bin/python3.11',
		'imageplotter.py',
		*[str(x) for x in inputs["input_fits"]],
		str(inputs["output_image"]),
	]
	print("Running:", *cmd)
	subprocess.run(cmd, env=tool_env, cwd=cwd)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context, cwd),
	}


def _process_images(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: Workflow
	label: process_images
	"""
	# Expression functions: js_eval should be replaced with Python
	def noiseremover_output_file_name(context):
		return js_eval("'no_noise_' + inputs.input.basename", context)

	# Create a clean temporary working directory and switch to it
	workflow_env, cwd = checkout(env)

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	wf_context = {"inputs": inputs} | context

	# Step ID:    imageplotter
	# Step label: imageplotter
	imageplotter_in = {
		"input_fits": inputs["fit_list"],
		"output_image": "before_noise_remover.png",
	}
	imageplotter_out = _imageplotter(imageplotter_in, context, env)

	# Step ID:    noiseremover
	# Step label: noiseremover
	noiseremover_in = {
		"input": inputs["fit_list"],
	}
	noiseremover_scattered_out = []
	for scattered_inputs in scatterizer(noiseremover_in, "input"):
		wf_context["inputs"] = inputs | scattered_inputs
		scattered_inputs["output_file_name"] = noiseremover_output_file_name(wf_context)
		noiseremover_scattered_out.append(_noiseremover(scattered_inputs, context, env))
	noiseremover_out = transpose(noiseremover_scattered_out)

	# Step ID:    after_plot_inspect
	# Step label: imageplotter
	after_plot_inspect_in = {
		"input_fits": noiseremover_out["output"],
		"output_image": "after_noise_remover.png",
	}
	after_plot_inspect_out = _imageplotter(after_plot_inspect_in, context, env)

	# Compute outputs
	return {
		"before_noise_remover": imageplotter_out["output"],
		"after_noise_remover_plot": after_plot_inspect_out["output"],
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

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

	# Submit to DASK
	result = client.compute(_process_images(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
