from PWF.src.commandlinetool import BaseCommandLineTool

class InitialWorkDirRequirement_PWF(BaseCommandLineTool):

	def set_inputs(self):
		self.inputs = {
			"message": {
				"type": "string",
			},
			"stage": {
				"type": "file",
				"default": {
					"path": "/home/kennethdirker/Leiden/2024-2025/Thesis/transpile/PWF/PWF/example/features/tools/InitialWorkDirRequirement.yaml",
				}
			},
		}

	def set_outputs(self):
		self.outputs = {}

	def set_base_command(self):
		self.base_command = [
			"sh",
			"example.sh",
			";",
			"cat",
			"InitialWorkDirRequirement.yaml",
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
				{
					"entry": "$(inputs.stage)",
				},
			],
			"ShellCommandRequirement": True,
		}

if __name__ == "__main__":
	InitialWorkDirRequirement_PWF()