import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, js_eval, load_input_object
from dask.distributed import Client

@dask.delayed
def imageplotter(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	label: imageplotter
	"""
	def outputs_output(context):
		pattern = js_eval("inputs.output_image", context)
		return FileObject(glob(pattern)[0])

	# Gather inputs in their correct format
	inputs = {
		"input_fits": None,
		"output_image": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Ready the commandline and execute the tool
	cmd = [
		'python',
		'scripts/imageplotter.py',
		*[str(v) for v in inputs["input_fits"]],
		str(inputs["output_image"]),
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
	result = client.compute(imageplotter(input_obj, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
