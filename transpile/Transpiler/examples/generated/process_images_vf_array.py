import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, js_eval, load_input_object
from dask.distributed import Client

@dask.delayed
def imageplotter(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	label: imageplotter
	"""
	def outputs_output(context):
		pattern = js_eval("inputs.output_image", context)
		return FileObject(glob(pattern)[0])

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'python',
		'/home/kennethdirker/Leiden/2024-2025/Thesis/transpile/Transpiler/examples/cwl/scripts/imageplotter.py',
		*[str(v) for v in inputs["input_fits"]],
		str(inputs["output_image"]),
	]
	print("Running:",  *cmd)
	subprocess.run(cmd)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


def process_images_vf_array(input_obj: dict, context: dict) -> dict:
	"""
	class: Workflow
	label: process_images_vf_array
	"""
	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Step ID:    imageplotter
	# Step label: imageplotter
	imageplotter_in = {
		"input_fits": [
			FileObject({"path":"file:///home/kennethdirker/Leiden/2024-2025/Thesis/transpile/Transpiler/examples/cwl/workflows/image_mf_01.fits"}),
			FileObject({"path":"file:///home/kennethdirker/Leiden/2024-2025/Thesis/transpile/Transpiler/examples/cwl/workflows/image_mf_02.fits"}),
			FileObject({"path":"file:///home/kennethdirker/Leiden/2024-2025/Thesis/transpile/Transpiler/examples/cwl/workflows/image_mf_04.fits"}),
		],
		"output_image": "before_noise_remover.png",
	}
	imageplotter_out = imageplotter(imageplotter_in, context)

	# Compute outputs
	return {
		"before_noise_remover": imageplotter_out["output"],
	}


def main():
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(process_images_vf_array(input_obj, context)).result()
	print(*[f"{k}: {v}" for k, v in result.items()], sep="\n")

if __name__ == "__main__":
	main()
