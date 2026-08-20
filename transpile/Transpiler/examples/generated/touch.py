import dask, subprocess
from CWL2DASK.scripting import (
FileObject,
	checkout,
	glob,
	js_eval,
	process_cli_args,
	publish_output
)
from dask.distributed import Client


@dask.delayed
def touch(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

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
		str(inputs["filename"]),
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
	input_obj, env = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(touch(input_obj, {}, env)).result()
	print(publish_output(result))

if __name__ == "__main__":
	main()
