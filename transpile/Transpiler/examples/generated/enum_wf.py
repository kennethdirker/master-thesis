import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, js_eval, load_input_object
from dask.distributed import Client

@dask.delayed
def enum(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	"""
	def outputs_out(context):
		matches = glob("enum.stdout")
		context["self"] = [FileObject(m, loadContents = True) for m in matches]
		return js_eval("self[0].contents", context)

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'echo',
		str(inputs["bound"]),
	]
	stdout = open("enum.stdout", "w")
	print("Running:",  *cmd)
	subprocess.run(
		args=cmd,
		stdout=stdout,
	)
	stdout.close()

	# Collect and generate outputs
	return {
		"out": outputs_out(tool_context),
	}


def enum_wf(input_obj: dict, context: dict) -> dict:
	"""
	class: Workflow
	"""
	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Step ID:    enum
	enum_in = {
		"bound": inputs["foo"],
	}
	enum_out = enum(enum_in, context)

	# Compute outputs
	return {
		"wf_output": enum_out["out"],
	}


def main():
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(enum_wf(input_obj, context)).result()
	print(*[f"{k}: {v}" for k, v in result.items()], sep="\n")

if __name__ == "__main__":
	main()
