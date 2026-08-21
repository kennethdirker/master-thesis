import dask, subprocess
from CWL2DASK.scripting import (
	FileObject,
	checkout,
	finalize,
	glob,
	initial_work_dir_requirement,
	js_eval,
	process_cli_args,
)
from dask.distributed import Client


@dask.delayed
def losoto_clocktec(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

	js_context = [
		"/**",
		" * A merely illustrative example function that uses a function",
		" * from the included custom-functions.js file to create a",
		" * Hello World message.",
		" *",
		" * @param {Object} message - CWL document input message",
		" */",
		"var createHelloWorldMessage = function (message) {",
		"  return capitalizeWords(message);",
		"};",
	]

	def stage_expr_0(context):
		return js_eval("get_losoto_config('CLOCKTEC').join('\n')", context, js_context)
	def stage_expr_1(context):
		return js_eval("inputs.input_h5parm.basename", context, js_context)
	def stage_expr_2(context):
		return js_eval("inputs.input_h5parm", context, js_context)
	def expr_handler_0(context: dict) -> str:
		return js_eval("inputs.input_h5parm.basename", context, js_context)
	def stdout_handler(context):
		return js_eval("inputs.input_h5parm.basename + '-losoto.log'", context, js_context)
	def stderr_handler(context):
		return js_eval("inputs.input_h5parm.basename + '-losoto_err.log'", context, js_context)
	def outputs_output_h5parm(context):
		pattern = js_eval("inputs.input_h5parm.basename", context, js_context)
		return FileObject(glob(pattern)[0])
	def outputs_parset(context):
		return FileObject(glob("parset.config")[0])
	def outputs_log(context):
		pattern = js_eval("inputs.input_h5parm.basename + '-losoto*.log'", context, js_context)
		return FileObject(glob(pattern))

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Stage files and directories to the temporary working directory
	initial_work_dir_requirement([
		{
			"entryname": "parset.config",
			"entry": stage_expr_0(tool_context),
		},
		{
			"entryname": stage_expr_1(tool_context),
			"entry": stage_expr_2(tool_context),
			"writable": "True",
		},
	])

	# Ready the commandline and execute the tool
	cmd = [
		'losoto',
		"--verbose",
		str(expr_handler_0(tool_context)),
		"parset.config",
	]
	stdout = open(stdout_handler(tool_context), "w")
	stderr = open(stderr_handler(tool_context), "w")
	cmd = [x for x in cmd if x]
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		env=env,
		stdout=stdout,
		stderr=stderr,
	)
	stdout.close()
	stderr.close()

	# Collect and generate outputs
	return {
		"output_h5parm": outputs_output_h5parm(tool_context),
		"parset": outputs_parset(tool_context),
		"log": outputs_log(tool_context),
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(losoto_clocktec(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
