from PWF.src.workflow import BaseWorkflow

class multilevel_scatter_PWF(BaseWorkflow):

	def set_metadata(self):
		self.metadata = {
			"label": "multilevel_scatter_example",
		}

	def set_inputs(self):
		self.inputs = {
			"words": {
				"type": "string[]",
			},
		}

	def set_outputs(self):
		self.outputs = {
			"cross_echoes": {
				"type": "string[]",
				"outputSource": "combine_cross/content_logs",
			},
			"cross_files": {
				"type": "file[]",
				"outputSource": "combine_cross/output_files",
			},
			"dot_echoes": {
				"type": "string[]",
				"outputSource": "combine_dot/content_logs",
			},
			"dot_files": {
				"type": "file[]",
				"outputSource": "combine_dot/output_files",
			},
		}

	def set_steps(self):
		self.steps = {
			"combine_dot": {
				"in": {
					"str": {
						"source": "words",
					},
					"num": {
						"default": [1, 2, 3],
					},
				},
				"scatter": [
					"str",
					"num",
				],
				"scatterMethod": "dotproduct",
				"out": [
					"output_files",
					"content_logs",
				],
				"run": "multilevel_scatter_create_echo.py",
			},
			"combine_cross": {
				"in": {
					"str": {
						"source": "words",
					},
					"num": {
						"default": [1, 2],
					},
				},
				"scatter": [
					"str",
					"num",
				],
				"scatterMethod": "flat_crossproduct",
				"out": [
					"output_files",
					"content_logs",
				],
				"run": "multilevel_scatter_create_echo.py",
			},
		}

if __name__ == "__main__":
	multilevel_scatter_PWF()