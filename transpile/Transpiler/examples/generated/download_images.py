import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, load_input_object
from dask.distributed import Client

@dask.delayed
def download_images(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	label: download_images
	"""
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
	subprocess.run(cmd)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


def main():
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(download_images(input_obj, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
