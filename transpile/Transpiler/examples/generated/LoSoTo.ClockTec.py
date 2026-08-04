import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, js_eval, load_input_object
from dask.distributed import Client

@dask.delayed
def losoto_clocktec(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	def expr_handler_0(context: dict) -> str:
		return js_eval("inputs.input_h5parm.basename", context)
	def stdout_handler(context):
		return js_eval("inputs.input_h5parm.basename + '-losoto.log'", context)
	def stderr_handler(context):
		return js_eval("inputs.input_h5parm.basename + '-losoto_err.log'", context)
	def outputs_output_h5parm(context):
		pattern = js_eval("inputs.input_h5parm.basename", context)
		return FileObject(glob(pattern)[0])
	def outputs_parset(context):
		return FileObject(glob("parset.config")[0])
	def outputs_log(context):
		pattern = js_eval("inputs.input_h5parm.basename + '-losoto*.log'", context)
		return FileObject(glob(pattern))

	# Gather inputs in their correct format
	inputs = {
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Ready the commandline and execute the tool
	cmd = [
		'losoto',
		"--verbose",
		str(expr_handler_0(tool_context)),
		"parset.config",
	]
	stdout = open(stdout_handler(tool_context), "w")
	stderr = open(stderr_handler(tool_context), "w")
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
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
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(losoto_clocktec(input_obj, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
