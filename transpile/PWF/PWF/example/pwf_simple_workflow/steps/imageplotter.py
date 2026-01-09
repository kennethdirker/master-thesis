from PWF.src.commandlinetool import BaseCommandLineTool

class imageplotter_PWF(BaseCommandLineTool):

	def set_metadata(self):
		self.metadata = {
			"label": "imageplotter",
		}

	def set_inputs(self):
		self.inputs = {
			"input_fits": {
				"type": "file[]",
				"bound": True,
				"position": 0,
			},
			"output_image": {
				"type": "string",
				"bound": True,
				"position": 1,
			},
		}

	def set_outputs(self):
		self.outputs = {
			"output": {
				"type": "file",
				"glob": "$(inputs.output_image)",
			},
		}

	def set_base_command(self):
		self.base_command = [
			"python",
			"/home/kennethdirker/Leiden/2024-2025/Thesis/transpile/PWF/PWF/example/cwl_simple_workflow/scripts/imageplotter.py",
		]

	def set_io(self):
		self.io = {}

if __name__ == "__main__":
	imageplotter_PWF()