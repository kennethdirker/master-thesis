import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, js_eval, load_input_object
from dask.distributed import Client

@dask.delayed
def touch(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	def expr_handler_0(context: dict) -> str:
		return js_eval("'test_' + inputs.filename", context)
	def stdout_handler(context):
		return js_eval("inputs.filename", context)
	def outputs_file_content(context):
		pattern = js_eval("inputs.filename", context)
		matches = glob(pattern)
		context["self"] = [FileObject(m, loadContents = True) for m in matches]
		return js_eval("self[0].contents.trim().split()", context)
	def outputs_file_name(context):
		pattern = js_eval("inputs.filename", context)
		return FileObject(glob(pattern)[0])

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'echo',
		str(expr_handler_0(tool_context)),
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
		"file_content": outputs_file_content(tool_context),
		"file_name": outputs_file_name(tool_context),
	}


def main():
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(touch(input_obj, context)).result()
	print(*[f"{k}: {v}" for k, v in result.items()], sep="\n")

if __name__ == "__main__":
	main()
