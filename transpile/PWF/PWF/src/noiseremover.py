from PWF.src.CommandLineTool import BaseCommandLineTool

class noiseremover(BaseCommandLineTool):

	def set_metadata(self):
		self.label = "noiseremover"

	def set_inputs(self):
		self.inputs = {
			"input": {
				"type": "file",
				"position": 0,
			},
			"output_file_name": {
				"type": "string",
				"position": 1,
			},
		}

	def set_outputs(self):
		self.outputs = {
			"output": {
				"type": "file",
				"glob": "$output_file_name$",
			},
		}

	def set_base_command(self):
		self.base_command = [
			"python",
			"noiseremover.py",
		]

if __name__ == "__main__":
	noiseremover(main=True)