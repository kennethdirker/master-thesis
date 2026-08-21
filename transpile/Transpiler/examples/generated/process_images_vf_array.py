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
def imageplotter(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	label: imageplotter
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

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
	subprocess.run(cmd, env=env)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


def process_images_vf_array(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: Workflow
	label: process_images_vf_array
	"""
	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	wf_context = {"inputs": inputs} | context

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
	imageplotter_out = imageplotter(imageplotter_in, context, env)

	# Compute outputs
	return {
		"before_noise_remover": imageplotter_out["output"],
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(process_images_vf_array(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
