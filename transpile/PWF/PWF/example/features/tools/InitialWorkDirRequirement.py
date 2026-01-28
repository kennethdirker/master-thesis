from PWF.src.commandlinetool import BaseCommandLineTool

class InitialWorkDirRequirement_PWF(BaseCommandLineTool):

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
			"sh",
			"example.sh",
		]

	def set_requirements(self):
		self.requirements = {
			"InitialWorkDirRequirement": [
				{
					"entryname": "example.sh",
					"entry": [
						"PREFIX='Message is:'",
						"$('MSG=\"${PREFIX} ' + inputs.message + '\"')",
						"echo ${MSG}",
					],
				},
			],
		}

	def set_io(self):
		self.io = {}

if __name__ == "__main__":
	InitialWorkDirRequirement_PWF()