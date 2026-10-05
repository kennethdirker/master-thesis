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
def _echo(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	label: echo_to_stdout
	"""
	def outputs_outfile(context):
		return FileObject(glob("greeting.txt")[0])
	def outputs_outmessage(context):
		matches = glob("greeting.txt")
		context["self"] = [FileObject(m) for m in matches]
		return js_eval("self.contents", context)

	# Create a clean temporary working directory and switch to it
	checkout(env)

	# Gather inputs in their correct format
	inputs = {
		"surname": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'echo',
		"Greetings,",
		str(inputs["username"]),
		str(inputs["surname"]),
	]
	stdout = open("greeting.txt", "w")
	cmd = [x for x in cmd if x]
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		env=env,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"outfile": outputs_outfile(tool_context),
		"outmessage": outputs_outmessage(tool_context),
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(_echo(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
