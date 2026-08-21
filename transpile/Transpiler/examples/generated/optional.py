import dask, subprocess
from CWL2DASK.scripting import (
	FileObject,
	checkout,
	finalize,
	glob,
	js_eval,
	process_cli_args,
)
from dask.distributed import Client


@dask.delayed
def imageplotter(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	label: imageplotter
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

	def outputs_output(context):
		pattern = js_eval("inputs.output_image", context)
		return FileObject(glob(pattern)[0])

	# Gather inputs in their correct format
	inputs = {
		"output_image": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'python',
		'scripts/imageplotter.py',
		*[str(x) for x in inputs["input_fits"]],
		str(inputs["output_image"]),
	]
	cmd = [x for x in cmd if x]
	print("Running:",  *cmd)
	subprocess.run(cmd, env=env)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(imageplotter(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
