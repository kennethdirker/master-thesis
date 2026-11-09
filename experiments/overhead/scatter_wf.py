import dask, subprocess
from CWL2DASK.scripting import (
	checkout,
	finalize,
	js_eval,
	process_cli_args,
	scatterizer,
	transpose,
)
from dask.distributed import Client

from dask_jobqueue.slurm import SLURMCluster


@dask.delayed
def _no_op(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Expression functions: js_eval should be replaced with Python
	def outputs_output_int(context, cwd):
		return js_eval("inputs.input_int", context)

	# Create a clean temporary working directory and fitted environment dict
	tool_env, cwd = checkout(env)

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [":"]
	print("Running:", *cmd)
	subprocess.run(
		args=cmd,
		env=tool_env,
		cwd=cwd,
		shell=True,
	)

	# Collect and generate outputs
	return {
		"output_int": outputs_output_int(tool_context, cwd),
	}


def _scatter_no_op_wf(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: Workflow
	"""
	# Create a clean temporary working directory and switch to it
	workflow_env, cwd = checkout(env)

	# Gather inputs in their correct format
	inputs = {
		"range": [
			0,
			1,
			2,
			3,
			4,
			5,
			6,
			7,
			8,
			9,
		],
	}
	inputs.update(input_obj)
	wf_context = {"inputs": inputs} | context

	# Step ID:    scatter_step
	scatter_step_in = {
		"input_int": inputs["range"],
	}
	scatter_step_scattered_out = []
	for scattered_inputs in scatterizer(scatter_step_in, "input"):
		wf_context["inputs"] = inputs | scattered_inputs
		scatter_step_scattered_out.append(_no_op(scattered_inputs, context, env))
	scatter_step_out = transpose(scatter_step_scattered_out)

	# Compute outputs
	return {
		"output_int": scatter_step_out["output_int"],
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
	client=client()

	# Submit to DASK
	result = client.compute(_scatter_no_op_wf(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
