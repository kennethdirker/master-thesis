from PWF.src.commandlinetool import BaseCommandLineTool

class args_PWF(BaseCommandLineTool):

	def set_metadata(self):
		self.metadata = {}

	def set_inputs(self):
		self.inputs = {
			"beautiful": {
				"type": "string",
			},
			"world": {
				"type": "string",
			},
		}

	def set_outputs(self):
		self.outputs = {
			"output": {
				"type": "stdout",
				"glob": "output.txt",
				"streamable": True,
			}
		}

	def set_base_command(self):
		self.base_command = [
			"echo",
		]

	def set_arguments(self):
		self.arguments = [
			"Hello",
			{
				"valueFrom": "$(inputs.world)",
				"position": 2,
			},
			{
				"valueFrom": "$(inputs.beautiful)",
				"position": 1,
			},
		]

	def set_io(self):
		self.io = {
			"stdout": "output.txt",
		}

if __name__ == "__main__":
	args_PWF()