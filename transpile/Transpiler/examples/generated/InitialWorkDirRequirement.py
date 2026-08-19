import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, initial_work_dir_requirement, js_eval, load_input_object
from dask.distributed import Client

@dask.delayed
def InitialWorkDirRequirement(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	def stage_expr_0(context):
		return js_eval("'MSG=\"\${PREFIX} ' + inputs.message + '\"'", context)
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
				"echo \${MSG}",
			],
		},
		{
			"entry": stage_expr_1(tool_context),
		},
	])

	# Ready the commandline and execute the tool
	cmd = [
		'sh',
		'example.sh',
		';',
		'cat',
		'InitialWorkDirRequirement.yaml',
	]
	print("Running:",  *cmd)
	subprocess.run(cmd)

	# Collect and generate outputs
	return {
	}


def main():
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(InitialWorkDirRequirement(input_obj, context)).result()
	print(*[f"{k}: {v}" for k, v in result.items()], sep="\n")

if __name__ == "__main__":
	main()
