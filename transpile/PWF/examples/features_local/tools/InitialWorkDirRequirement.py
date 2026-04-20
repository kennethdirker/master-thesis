from PWF.commandlinetool import BaseCommandLineTool

class InitialWorkDirRequirement_PWF(BaseCommandLineTool):

	def set_inputs(self):
		self.inputs = {
			"message": {
				"type": "string",
			},
			"stage": {
				"type": "file",
				"default": {
					"class": "File",
<<<<<<<< HEAD:transpile/PWF/examples/features_cluster/tools/InitialWorkDirRequirement.py
					"path": "/home/kdirker/Thesis/master-thesis/transpile/PWF/examples/features_cluster/tools/InitialWorkDirRequirement.yaml",
========
					"path": "/home/kennethdirker/Leiden/2024-2025/Thesis/transpile/PWF/examples/features_local/tools/InitialWorkDirRequirement.yaml",
>>>>>>>> bcbf08f1283ed9b53a89e2bf577ef2fa9c258b60:transpile/PWF/examples/features_local/tools/InitialWorkDirRequirement.py
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