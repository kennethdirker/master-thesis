from PWF.src.CommandLineTool import BaseCommandLineTool

class download_images(BaseCommandLineTool):

	def set_metadata(self):
		self.label = "download_images"
		self.doc = (
			"This is a doc that is used to test the parsing behaviour of "
			"the PWF transpiler. We need to use strings that are quite lo"
			"ng, so that we might test some stuff related to string lengt"
			"h. Abadabada. Da ba a da ba, da ba da, da be da ba ba."
		)

	def set_inputs(self):
		self.inputs = {
			"url_list": {
				"type": "file",
				"prefix": "-i",
				"position": 0,
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
	download_images(main=True)