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
def touch(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

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
		env=env,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"file_content": outputs_file_content(tool_context),
		"file_name": outputs_file_name(tool_context),
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(touch(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
