import dask, subprocess
from CWL2DASK.scripting import (
	FileObject,
	checkout,
	finalize,
	glob,
	process_cli_args,
)
from dask.distributed import Client


@dask.delayed
def _stdout(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Expression functions: js_eval should be replaced with Python
	def outputs_example_out(context, cwd):
		return FileObject(glob("output.txt", cwd)[0])

	# Create a clean temporary working directory and fitted environment dict
	tool_env, cwd = checkout(env)

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'echo',
		str(inputs["message"]),
	]
	stdout = open("output.txt", "w")
	print("Running:", *cmd)
	subprocess.run(
		args=cmd,
		env=tool_env,
		cwd=cwd,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"example_out": outputs_example_out(tool_context, cwd),
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(_stdout(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
