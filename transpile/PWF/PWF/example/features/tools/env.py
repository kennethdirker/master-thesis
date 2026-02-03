from PWF.src.commandlinetool import BaseCommandLineTool

class env_PWF(BaseCommandLineTool):

	def set_metadata(self):
		self.metadata = {}

	def set_inputs(self):
		self.inputs = {
			"message": {
				"type": "string",
			},
		}

	def set_outputs(self):
		self.outputs = {}

	def set_base_command(self):
		self.base_command = [
			"env",
		]

	def set_requirements(self):
		self.requirements = {
			"EnvVarRequirement": {
				"HELLO": "$(inputs.message)",
			},
		}

	def set_io(self):
		self.io = {}

if __name__ == "__main__":
	env_PWF()