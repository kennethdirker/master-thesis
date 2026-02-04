from PWF.src.commandlinetool import BaseCommandLineTool

class multitype_PWF(BaseCommandLineTool):

	def set_metadata(self):
		self.metadata = {}

	def set_inputs(self):
		self.inputs = {
			"message": {
				"type": ["int", "string", "null"],
				"bound": True,
				"position": 0,
			},
		}

	def set_outputs(self):
		self.outputs = {}

	def set_base_command(self):
		self.base_command = [
			"echo",
		]

if __name__ == "__main__":
	multitype_PWF()