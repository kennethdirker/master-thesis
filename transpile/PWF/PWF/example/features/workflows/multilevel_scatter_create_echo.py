from PWF.src.workflow import BaseWorkflow

class multilevel_scatter_create_echo_PWF(BaseWorkflow):

	def set_metadata(self):
		self.metadata = {
			"label": "multilevel scatter example",
		}

	def set_inputs(self):
		self.inputs = {
			"str": {
				"type": "string",
			},
			"num": {
				"type": "int",
			},
		}

	def set_outputs(self):
		self.outputs = {
			"output_files": {
				"type": "file",
				"outputSource": "touch/file_name",
			},
			"content_logs": {
				"type": "string[]",
				"outputSource": "print/echo",
			},
		}
