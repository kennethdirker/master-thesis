from PWF.src.commandlinetool import BaseCommandLineTool

class hostname_PWF(BaseCommandLineTool):

	def set_metadata(self):
		self.metadata = {}

	def set_inputs(self):
		self.inputs = {}

	def set_outputs(self):
		self.outputs = {
			"hostname": {
				"type": "stdout",
				"glob": "hostname.txt",
				"streamable": True,
			}
		}

	def set_base_command(self):
		self.base_command = [
			"hostname",
		]

	def set_io(self):
		self.io = {
			"stdout": "hostname.txt",
		}

if __name__ == "__main__":
	hostname_PWF()