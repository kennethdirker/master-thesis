from PWF.src.commandlinetool import BaseCommandLineTool

class stdout_PWF(BaseCommandLineTool):

	def set_metadata(self):
		self.metadata = {}

	def set_inputs(self):
		self.inputs = {
			"message": {
				"type": "string",
				"bound": True,
				"position": 1,
			},
		}

	def set_outputs(self):
		self.outputs = {
			"example_out": {
				"type": "stdout",
				"glob": "output.txt",
				"streamable": True,
			}
		}

	def set_base_command(self):
		self.base_command = [
			"echo",
		]

	def set_io(self):
		self.io = {
			"stdout": "output.txt",
		}

if __name__ == "__main__":
	stdout_PWF()