from PWF.src.commandlinetool import BaseCommandLineTool

class download_images_PWF(BaseCommandLineTool):

	def set_metadata(self):
		self.metadata = {
			"label": "download_images",
		}

	def set_inputs(self):
		self.inputs = {
			"url_list": {
				"type": "file",
				"bound": True,
				"position": 0,
				"prefix": "-i",
			},
		}

	def set_outputs(self):
		self.outputs = {
			"output": {
				"type": "file[]",
				"glob": "*.fits",
			},
		}

	def set_base_command(self):
		self.base_command = [
			"wget",
		]

if __name__ == "__main__":
	download_images_PWF()