from PWF.commandlinetool import BaseCommandLineTool

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
			"imageplotter.py",
		]

	def set_requirements(self):
		self.requirements = {
			"InitialWorkDirRequirement": [
				{
					"class": "File",
					"location": "/home/kdirker/Thesis/master-thesis/transpile/PWF/examples/cwl_local_workflow/scripts/imageplotter.py",
				},
			],
		}

if __name__ == "__main__":
	imageplotter_PWF()