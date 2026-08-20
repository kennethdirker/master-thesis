import dask, subprocess
from CWL2DASK.scripting import (
FileObject,
	checkout,
	glob,
	js_eval,
	process_cli_args,
	publish_output
)
from dask.distributed import Client


@dask.delayed
def enum(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

	def outputs_out(context):
		matches = glob("enum.stdout")
		context["self"] = [FileObject(m, loadContents = True) for m in matches]
		return js_eval("self[0].contents", context)

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'echo',
		str(inputs["bound"]),
	]
	stdout = open("enum.stdout", "w")
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		env=env,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"out": outputs_out(tool_context),
	}


def enum_wf(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: Workflow
	"""
	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	wf_context = {"inputs": inputs} | context

	# Step ID:    enum
	enum_in = {
		"bound": inputs["foo"],
	}
	enum_out = enum(enum_in, context, env)

	# Compute outputs
	return {
		"wf_output": enum_out["out"],
	}


def main():
	# Process program parameters
	input_obj, env = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(enum_wf(input_obj, {}, env)).result()
	print(publish_output(result))

if __name__ == "__main__":
	main()
