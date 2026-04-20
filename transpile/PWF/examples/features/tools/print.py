from PWF.commandlinetool import BaseCommandLineTool

class print_PWF(BaseCommandLineTool):

	def set_inputs(self):
		self.inputs = {
			"str": {
				"type": "string",
				"bound": True,
				"position": 0,
			},
		}

	def set_outputs(self):
		self.outputs = {
			"echo": {
				"type": "string",
				"glob": "$(inputs.str)",
				"loadContents": True,
				"outputEval": "$(self[0].contents.trim())",
			},
		}

	def set_base_command(self):
		self.base_command = [
			"echo",
		]

	def set_io(self):
		self.io = {
			"stdout": "$(inputs.str)",
		}

if __name__ == "__main__":
	print_PWF()