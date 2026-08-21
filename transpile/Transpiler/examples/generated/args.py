import dask, subprocess
from CWL2DASK.scripting import (
	FileObject,
	checkout,
	finalize,
	glob,
	js_eval,
	process_cli_args,
)
from dask.distributed import Client


@dask.delayed
def args(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

	def expr_handler_0(context: dict) -> str:
		expr = [
			"This is some",
			"Lorem Ipsum",
			"'type ' + inputs.food + \"digestion\" + ' stuff'",
			"Peace out!",
		]
		return js_eval(expr, context)
	def expr_handler_1(context: dict) -> str:
		return js_eval("inputs.world", context)
	def expr_handler_2(context: dict) -> str:
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
		str(expr_handler_0(tool_context)),
		"#" + expr_handler_2(tool_context),
		str(expr_handler_1(tool_context)),
		"Bazinga",
	]
	stdout = open("output.txt", "w")
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		env=env,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(args(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
