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
def env(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

	def env_HELLO(context):
		return js_eval(inputs.message, context)
	def outputs_example_out(context):
		return FileObject(glob("output.txt")[0])

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = ['env']
	stdout = open("output.txt", "w")
	env = {"HELLO": env_HELLO(tool_context)}
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		env=env,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"example_out": outputs_example_out(tool_context),
	}


def main():
	# Process program parameters
	input_obj, env = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(env(input_obj, {}, env)).result()
	print(publish_output(result))

if __name__ == "__main__":
	main()
