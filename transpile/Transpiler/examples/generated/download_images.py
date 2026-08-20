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
def download_images(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	label: download_images
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

	def outputs_output(context):
		return FileObject(glob("*.fits"))

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'wget',
		"-i" + inputs["url_list"],
	]
	print("Running:",  *cmd)
	subprocess.run(cmd, env=env)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


def main():
	# Process program parameters
	input_obj, env = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(download_images(input_obj, {}, env)).result()
	print(publish_output(result))

if __name__ == "__main__":
	main()
