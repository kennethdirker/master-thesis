from PWF.src.Workflow import BaseWorkflow

class process_images(BaseWorkflow):

	def set_metadata(self):
		self.label = "process_images"

	def set_inputs(self):
		self.inputs = {
			"url_list": {
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
				"label": "imageplotter",
				"in": {
					"input_fits": {
						"source": "download_images/output",
					},
					"output_image": {
						"default": "before_noise_remover.png",
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/imageplotter.cwl",
			},
			"noiseremover": {
				"label": "noiseremover",
				"in": {
					"input": {
						"source": "download_images/output",
					},
					"output_file_name": {
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/noiseremover.cwl",
			},
			"download_images": {
				"label": "download_images",
				"in": {
					"url_list": {
						"source": "url_list",
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/download_images.cwl",
			},
			"after_plot_inspect": {
				"label": "imageplotter",
				"in": {
					"input_fits": {
						"source": "noiseremover/output",
					},
					"output_image": {
						"default": "after_noise_remover.png",
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/imageplotter.cwl",
			},
		}

if __name__ == "__main__":
	process_images(main=True)