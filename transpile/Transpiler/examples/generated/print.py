import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, js_eval, load_input_object
from dask.distributed import Client

@dask.delayed
def print(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	def stdout_handler(context):
		return js_eval("inputs.str", context)
	def outputs_echo(context):
		pattern = js_eval("inputs.str", context)
		matches = glob(pattern)
		context["self"] = [FileObject(m, loadContents = True) for m in matches]
		return js_eval("self[0].contents.trim()", context)

	# Gather inputs in their correct format
	inputs = {
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Ready the commandline and execute the tool
	cmd = [
		'echo',
		str(inputs["str"]),
	]
	stdout = open(stdout_handler(tool_context), "w")
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"echo": outputs_echo(tool_context),
	}


def main():
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(print(input_obj, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
