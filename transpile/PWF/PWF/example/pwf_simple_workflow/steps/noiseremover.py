from PWF.src.commandlinetool import BaseCommandLineTool

class noiseremover_PWF(BaseCommandLineTool):

	def set_metadata(self):
		self.metadata = {
			"label": "noiseremover",
		}

	def set_inputs(self):
		self.inputs = {
			"input": {
				"type": "file",
				"bound": True,
				"position": 0,
			},
			"output_file_name": {
				"type": "string",
				"bound": True,
				"position": 1,
			},
		}

	def set_outputs(self):
		self.outputs = {
			"output": {
				"type": "file",
				"glob": "$(inputs.output_file_name)",
			},
		}

	def set_base_command(self):
		self.base_command = [
			"python",
			"/home/kennethdirker/Leiden/2024-2025/Thesis/transpile/PWF/PWF/example/cwl_simple_workflow/scripts/noiseremover.py",
		]

	def set_requirements(self):
		self.requirements = {}

	def set_io(self):
		self.io = {}

if __name__ == "__main__":
	noiseremover_PWF()