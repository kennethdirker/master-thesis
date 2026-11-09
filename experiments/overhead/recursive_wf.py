import dask, subprocess
from CWL2DASK.scripting import (
	checkout,
	finalize,
	first_non_null,
	js_eval,
	process_cli_args,
)
from dask.distributed import Client


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
	# Expression functions: js_eval should be replaced with Python
	def recursive_step_input_int(context):
		return js_eval("self - 1", context)
	def tool_step_when(context):
		return js_eval("inputs.i === 0", context)

	# Create a clean temporary working directory and switch to it
	workflow_env, cwd = checkout(env)

	# Gather inputs in their correct format
	inputs = {
		"i": 10,
	}
	inputs.update(input_obj)
	wf_context = {"inputs": inputs} | context

	# Step ID:    recursive_step
	recursive_step_in = {
		"input_int": inputs["i"],
	}
	wf_context["inputs"] = inputs | recursive_step_in
	recursive_step_in["input_int"] = recursive_step_input_int(wf_context | {"self": recursive_step_in["input_int"]})
	recursive_step_out = _no_op(recursive_step_in, context, env)

	# Step ID:    tool_step
	tool_step_in = {
		"input_int": inputs["i"],
	}
	wf_context["inputs"] = tool_step_in
	if tool_step_when(wf_context):
		tool_step_out = _no_op(tool_step_in, context, env)
	else:
		tool_step_out = {
			"output_int": None,
		}

	# Compute outputs
	return {
		"output_int": first_non_null([recursive_step_out["output_int"], tool_step_out["output_int"]]),
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(_scatter_no_op_wf(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
