import dask, subprocess, sys
from CWL2DASK.scripting import FileObject, glob, js_eval, load_input_object, scatterizer, transpose
from dask.distributed import Client

@dask.delayed
def noiseremover(input_obj: dict, context: dict) -> dict:
	"""
	class: CommandLineTool
	label: noiseremover
	"""
	def outputs_output(context):
		pattern = js_eval("inputs.output_file_name", context)
		return FileObject(glob(pattern)[0])

	# Gather inputs in their correct format
	inputs = {
		"input": None,
		"output_file_name": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Ready the commandline and execute the tool
	cmd = [
		'python',
		'noiseremover.py',
		str(inputs["input"]),
		str(inputs["output_file_name"]),
	]
	print("Running:",  *cmd)
	subprocess.run(cmd)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


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
	inputs = {
		"input_fits": None,
		"output_image": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Ready the commandline and execute the tool
	cmd = [
		'python',
		'scripts/imageplotter.py',
		*[str(v) for v in inputs["input_fits"]],
		str(inputs["output_image"]),
	]
	print("Running:",  *cmd)
	subprocess.run(cmd)

	# Collect and generate outputs
	return {
		"output": outputs_output(tool_context),
	}


@dask.delayed
def process_images(input_obj: dict, context: dict) -> dict:
	"""
	class: Workflow
	label: process_images
	"""
	def noiseremover_output_file_name(context):
		context["self"] = None
		return js_eval("'no_noise_' + inputs.input.basename", context)

	# Gather inputs in their correct format
	inputs = {
		"fit_list": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Step ID:    imageplotter
	# Step label: imageplotter
	imageplotter_in = {
		"input_fits": inputs["fit_list"],
		"output_image": "before_noise_remover.png",
	}
	imageplotter_out = imageplotter(imageplotter_in, context)

	# Step ID:    noiseremover
	# Step label: noiseremover
	noiseremover_in = {
		"input": inputs["fit_list"],
	}
	noiseremover_scattered_out = []
	for scattered_inputs in scatterizer(noiseremover_in, "input"):
		tool_context["inputs"] = {**inputs, **scattered_inputs}
		scattered_inputs["output_file_name"] = noiseremover_output_file_name(tool_context)
		noiseremover_scattered_out.append(noiseremover(scattered_inputs, context))
	noiseremover_out = dask.delayed(transpose)(noiseremover_scattered_out)

	# Step ID:    after_plot_inspect
	# Step label: imageplotter
	after_plot_inspect_in = {
		"input_fits": noiseremover_out["output"],
		"output_image": "after_noise_remover.png",
	}
	after_plot_inspect_out = imageplotter(after_plot_inspect_in, context)

	# Compute outputs
	return {
		"before_noise_remover": imageplotter_out["output"].compute(),
		"after_noise_remover_plot": after_plot_inspect_out["output"].compute(),
	}


@dask.delayed
def top_process_images(input_obj: dict, context: dict) -> dict:
	"""
	class: Workflow
	label: process_images
	"""
	def noiseremover_input(context, self):
		context["self"] = self
		return js_eval("self[0]", context)
	def noiseremover_output_file_name(context):
		context["self"] = None
		return js_eval("'top_no_noise_' + inputs.input[0].basename", context)
	def after_plot_inspect_input_fits(context, self):
		context["self"] = self
		return js_eval("[self]", context)

	# Gather inputs in their correct format
	inputs = {
		"list_of_fits": None,
	}
	inputs.update(input_obj)
	tool_context = {"inputs": inputs, **context}

	# Step ID:    subworkflow
	# Step label: subworkflow
	subworkflow_in = {
		"fit_list": inputs["list_of_fits"],
	}
	subworkflow_out = process_images(subworkflow_in, context)

	# Step ID:    imageplotter
	# Step label: imageplotter
	imageplotter_in = {
		"input_fits": inputs["list_of_fits"],
		"output_image": "top_before_noise_remover.png",
	}
	imageplotter_out = imageplotter(imageplotter_in, context)

	# Step ID:    noiseremover
	# Step label: noiseremover
	noiseremover_in = {
		"input": inputs["list_of_fits"],
	}
	noiseremover_out = noiseremover(noiseremover_in, context)
	scattered_inputs["input"] = noiseremover_input(tool_context, noiseremover_in[{input_id}])
	scattered_inputs["output_file_name"] = noiseremover_output_file_name(tool_context)

	# Step ID:    after_plot_inspect
	# Step label: imageplotter
	after_plot_inspect_in = {
		"input_fits": noiseremover_out["output"],
		"output_image": "top_after_noise_remover.png",
	}
	after_plot_inspect_out = imageplotter(after_plot_inspect_in, context)
	scattered_inputs["input_fits"] = after_plot_inspect_input_fits(tool_context, after_plot_inspect_in[{input_id}])

	# Compute outputs
	return {
		"before_noise_remover": subworkflow_out["before_noise_remover"].compute(),
		"after_noise_remover": subworkflow_out["after_noise_remover_plot"].compute(),
		"top_before_noise_remover_plot": imageplotter_out["output"].compute(),
		"top_after_noise_remover_plot": after_plot_inspect_out["output"].compute(),
	}


def main():
	# Initialize cluster
	client = Client()

	# Convert input YAML to dict
	input_obj = load_input_object(sys.argv[1])

	# Initialize CWL context
	context = {}

	# Submit to DASK
	result = client.compute(top_process_images(input_obj, context)).result()
	print(*[f'{k}: {v}' for k, v in result.items()])

if __name__ == "__main__":
	main()
