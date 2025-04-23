from PWF.src.CommandLineTool import BaseCommandLineTool

class imageplotter(BaseCommandLineTool):

	def set_metadata(self):
		self.label = "imageplotter"

	def set_inputs(self):
		self.inputs = {
			"input_fits": {
				"type": "file[]",
				"position": 0,
			},
			"output_image": {
				"type": "string",
				"position": 1,
			},
		}

	def set_outputs(self):
		self.outputs = {
			"output": {
				"type": "file",
				"glob": "$output_image$",
			},
		}

	def set_base_command(self):
		self.base_command = [
			"python",
			"../scripts/imageplotter.py",
		]

if __name__ == "__main__":
	imageplotter(main=True)