import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, load_input_object
from dask.distributed import Client

@dask.delayed
def stdout(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	def outputs_example_out(context):
		return FileObject(glob("output.txt")[0])

	# Gather inputs in their correct format
	inputs = {
		"message": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Ready the commandline and execute the tool
	cmd = [
		'echo',
		str(inputs["message"]),
	]
	print("Running:",  *cmd)
	stdout = open("output.txt", "w")
	subprocess.run(
		args=cmd,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"example_out": outputs_example_out(tool_context),
	}


def main():
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(stdout(input_obj, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
