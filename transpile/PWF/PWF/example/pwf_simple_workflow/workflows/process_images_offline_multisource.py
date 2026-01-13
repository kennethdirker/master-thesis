from PWF.src.workflow import BaseWorkflow

class process_images_offline_multisource_PWF(BaseWorkflow):

	def set_metadata(self):
		self.metadata = {
			"label": "process_images",
		}

	def set_inputs(self):
		self.inputs = {
			"fit_list": {
				"type": "file[]",
			},
			"fit_1": {
				"type": "file",
			},
			"fit_2": {
				"type": "file",
			},
			"fit_3": {
				"type": "file",
			},
		}

	def set_outputs(self):
		self.outputs = {
			"before_noise_remover": {
				"type": "file",
			},
			"after_noise_remover_plot": {
				"type": "file",
			},
		}

	def set_steps(self):
		self.steps = {
			"imageplotter": {
				"in": {
					"input_fits": {
						"source": "fit_list",
					},
					"output_image": {
						"default": "before_noise_remover.png",
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/imageplotter.py",
				"label": "imageplotter",
			},
			"noiseremover1": {
				"in": {
					"input": {
						"source": "fit_1",
					},
					"output_file_name": {
						"valueFrom": "$('no_noise_' + inputs.input.basename)"
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/noiseremover.py",
				"label": "noiseremover",
			},
			"noiseremover2": {
				"in": {
					"input": {
						"source": "fit_2",
					},
					"output_file_name": {
						"valueFrom": "$('no_noise_' + inputs.input.basename)"
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/noiseremover.py",
				"label": "noiseremover",
			},
			"noiseremover3": {
				"in": {
					"input": {
						"source": "fit_3",
					},
					"output_file_name": {
						"valueFrom": "$('no_noise_' + inputs.input.basename)"
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/noiseremover.py",
				"label": "noiseremover",
			},
			"after_plot_inspect": {
				"in": {
					"input_fits": {
						"source": [
							"noiseremover1/output",
							"noiseremover2/output",
							"noiseremover3/output",
						],
					},
					"output_image": {
						"default": "after_noise_remover.png",
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/imageplotter.py",
				"label": "imageplotter",
			},
		}


if __name__ == "__main__":
	process_images_offline_multisource_PWF()