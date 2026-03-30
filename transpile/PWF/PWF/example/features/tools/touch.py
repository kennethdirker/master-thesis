from PWF.src.commandlinetool import BaseCommandLineTool

class touch_PWF(BaseCommandLineTool):

	def set_inputs(self):
		self.inputs = {
			"filename": {
				"type": "string",
				"bound": True,
				"position": 0,
			},
		}

	def set_outputs(self):
		self.outputs = {
			"file_content": {
				"type": "string[]",
				"glob": "$(inputs.filename)",
				"loadContents": True,
				"outputEval": "$(self[0].contents.trim().split())",
			},
			"file_name": {
				"type": "stdout",
				"glob": "$(inputs.filename)",
				"streamable": True,
			}
		}

	def set_base_command(self):
		self.base_command = [
			"echo",
		]

	def set_io(self):
		self.io = {
			"stdout": "$(inputs.filename)",
		}

if __name__ == "__main__":
	touch_PWF()