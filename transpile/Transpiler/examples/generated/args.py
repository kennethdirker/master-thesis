import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, js_eval, load_input_object
from dask.distributed import Client

@dask.delayed
def args(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	def expr_handler_0(context: dict) -> str:
		return js_eval("inputs.world", context)
	def expr_handler_1(context: dict) -> str:
		return js_eval("inputs.beautiful", context)
	def outputs_output(context):
		return FileObject(glob("output.txt")[0])

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'echo',
		"Hello",
		"there",
		"#" + expr_handler_1(tool_context),
		str(expr_handler_0(tool_context)),
		"Bazinga",
	]
	stdout = open("output.txt", "w")
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		stdout=stdout,
	)
	stdout.close()

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
	result = client.compute(args(input_obj, context)).result()
	print(*[f"{k}: {v}" for k, v in result.items()], sep="\n")

if __name__ == "__main__":
	main()
