import dask, subprocess
from CWL2DASK.scripting import (
FileObject,
	checkout,
	glob,
	process_cli_args,
	publish_output
)
from dask.distributed import Client


@dask.delayed
def hostname(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

	def outputs_hostname(context):
		return FileObject(glob("hostname.txt")[0])

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = ['hostname']
	stdout = open("hostname.txt", "w")
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		env=env,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"hostname": outputs_hostname(tool_context),
	}


def main():
	# Process program parameters
	input_obj, env = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(hostname(input_obj, {}, env)).result()
	print(publish_output(result))

if __name__ == "__main__":
	main()
