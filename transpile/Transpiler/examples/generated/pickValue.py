import dask, subprocess
from CWL2DASK.scripting import (
	FileObject,
	all_non_null,
	checkout,
	finalize,
	glob,
	js_eval,
	merge_flattened,
	merge_nested,
	process_cli_args,
	scatterizer,
	transpose,
)
from dask.distributed import Client


@dask.delayed
def noiseremover(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: CommandLineTool
	label: noiseremover
	"""
	# Create a clean temporary working directory for this tool and switch to it
	checkout(env)

	def outputs_output(context):
		pattern = js_eval("inputs.output_file_name", context)
		return FileObject(glob(pattern)[0])

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs} | context

	# Ready the commandline and execute the tool
	cmd = [
		'python',
		'/home/kennethdirker/Leiden/2024-2025/Thesis/transpile/Transpiler/examples/cwl/scripts/noiseremover.py',
		str(inputs["input"]),
		str(inputs["output_file_name"]),
	]
	print("Running:",  *cmd)
	subprocess.run(cmd, env=env)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


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


def process_images(input_obj: dict, context: dict, env: dict) -> dict:
	"""
	class: Workflow
	label: process_images
	"""
	def noiseremover_output_file_name(context):
		return js_eval("'no_noise_' + inputs.input.basename", context)
	def mockup_when(context):
		return js_eval("false", context)
	def mockup_output_file_name(context):
		return js_eval("'no_noise_mockup_' + inputs.input.basename", context)

	# Gather inputs in their correct format
	inputs = {}
	inputs.update(input_obj)
	wf_context = {"inputs": inputs} | context

	# Step ID:    imageplotter
	# Step label: imageplotter
	imageplotter_in = {
		"input_fits": inputs["fit_list"],
		"output_image": "before_noise_remover.png",
	}
	imageplotter_out = imageplotter(imageplotter_in, context, env)

	# Step ID:    noiseremover
	# Step label: noiseremover
	noiseremover_in = {
		"input": inputs["fit_list"],
	}
	noiseremover_scattered_out = []
	for scattered_inputs in scatterizer(noiseremover_in, "input"):
		wf_context["inputs"] = inputs | scattered_inputs
		scattered_inputs["output_file_name"] = noiseremover_output_file_name(wf_context)
		noiseremover_scattered_out.append(noiseremover(scattered_inputs, context, env))
	noiseremover_out = dask.delayed(transpose)(noiseremover_scattered_out)

	# Step ID:    mockup
	# Step label: mockup
	mockup_in = {
		"input": noiseremover_out["output"],
	}
	wf_context["inputs"] = mockup_in
	if mockup_when(wf_context):
		mockup_scattered_out = []
		for scattered_inputs in scatterizer(mockup_in, "input"):
			wf_context["inputs"] = inputs | scattered_inputs
			scattered_inputs["output_file_name"] = mockup_output_file_name(wf_context)
			mockup_scattered_out.append(noiseremover(scattered_inputs, context, env))
		mockup_out = dask.delayed(transpose)(mockup_scattered_out)
	else:
		mockup_out = {
			"output": None,
		}

	# Step ID:    after_plot_inspect
	# Step label: imageplotter
	after_plot_inspect_in = {
		"input_fits": all_non_null(merge_flattened(noiseremover_out["output"], mockup_out["output"], inputs["fit_list"])),
		"output_image": "after_noise_remover.png",
	}
	after_plot_inspect_out = imageplotter(after_plot_inspect_in, context, env)

	# Compute outputs
	return {
		"before_noise_remover": imageplotter_out["output"],
		"after_noise_remover_plot": after_plot_inspect_out["output"],
		"merged": all_non_null(merge_nested(imageplotter_out["output"], after_plot_inspect_out["output"])),
	}


def main():
	# Process program parameters
	input_obj, env, preserve_tmpdir = process_cli_args()

	# Initialize cluster
	client = Client()

	# Submit to DASK
	result = client.compute(process_images(input_obj, {}, env)).result()
	print(finalize(result, env, preserve_tmpdir))

if __name__ == "__main__":
	main()
