import dask, subprocess
from CWL2DASK.scripting import (
	FileObject,
	checkout,
	finalize,
	initial_work_dir_requirement,
	js_eval,
	process_cli_args,
)
from dask.distributed import Client


@dask.delayed
def InitialWorkDirRequirement(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

	def stage_expr_0(context):
		return js_eval("'MSG=\"${PREFIX} ' + inputs.message + '\"'", context)
	def stage_expr_1(context):
		return js_eval("inputs.stage", context)

	# Gather inputs in their correct format
	inputs = {
		"stage": FileObject({"path":"InitialWorkDirRequirement.yaml"}),
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Stage files and directories to the temporary working directory
	initial_work_dir_requirement([
		{
			"entryname": "example.sh",
			"entry": [
				"PREFIX='Message is:'",
				stage_expr_0(tool_context),
				"echo ${MSG}",
			],
		},
		{
			"entry": stage_expr_1(tool_context),
		},
	])

	# Ready the commandline and execute the tool
	cmd = [' \
		"example.sh" \
		";" \
		"cat" \
		"InitialWorkDirRequirement.yaml" \
	']
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		env=env,
		shell=True,
	)

	# Collect and generate outputs
	return {
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(InitialWorkDirRequirement(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
