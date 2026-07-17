import dask, subprocess, sys
from CWL2DASK.scripting import load_input_object
from dask.distributed import Client

@dask.delayed
def file:///home/kennethdirker/Leiden/2024-2025/Thesis/transpile/Transpiler/examples/cwl/steps/env.cwl(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	"""

	# Gather inputs in their correct format
	inputs = {
		"env.cwl#message": None,
	}
	inputs.update(input_obj)

	# Ready the commandline and execute the tool
	cmd = [
		'env',
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
	result = client.compute(file:///home/kennethdirker/Leiden/2024-2025/Thesis/transpile/Transpiler/examples/cwl/steps/env.cwl(input_obj, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
