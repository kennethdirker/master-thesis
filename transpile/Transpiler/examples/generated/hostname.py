import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, load_input_object
from dask.distributed import Client

@dask.delayed
def hostname(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	def outputs_hostname(context):
		return FileObject(glob("hostname.txt")[0])

	# Gather inputs in their correct format
	inputs = {
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Ready the commandline and execute the tool
	cmd = ['hostname']
	print("Running:",  *cmd)
	stdout = open("hostname.txt", "w")
	subprocess.run(
		args=cmd,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"hostname": outputs_hostname(tool_context),
	}


def main():
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(hostname(input_obj, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
