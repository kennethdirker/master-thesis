from PWF.workflow import BaseWorkflow

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

	def set_steps(self):
		self.steps = {
			"touch": {
				"in": {
					"str": {
						"source": "str",
					},
					"num": {
						"source": "num",
					},
					"filename": {
						"valueFrom": "$(inputs.str + inputs.num)"
					},
				},
				"out": [
					"file_name",
					"file_content",
				],
				"run": "../tools/touch.py",
			},
			"print": {
				"in": {
					"str": {
						"source": "touch/file_content",
					},
				},
				"scatter": "str",
				"out": ["echo"],
				"run": "../tools/print.py",
			},
		}

if __name__ == "__main__":
	multilevel_scatter_create_echo_PWF()