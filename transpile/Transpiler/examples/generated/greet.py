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
def _greet(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	label: greet_user
	"""
	# Expression functions: js_eval should be replaced with Python
	def outputs_outfile(context, cwd):
		return FileObject(glob("greeting.txt", cwd)[0])
	def outputs_outmessage(context, cwd):
		matches = glob("greeting.txt", cwd)
		context["self"] = [FileObject(m) for m in matches]
		return js_eval("self.contents", context)

	# Create a clean temporary working directory and fitted environment dict
	tool_env, cwd = checkout(env)

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
	print("Running:", *cmd)
	subprocess.run(
		args=cmd,
		env=tool_env,
		cwd=cwd,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"outfile": outputs_outfile(tool_context, cwd),
		"outmessage": outputs_outmessage(tool_context, cwd),
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(_greet(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
