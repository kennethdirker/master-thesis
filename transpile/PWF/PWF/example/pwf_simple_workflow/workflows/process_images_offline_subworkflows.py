from PWF.src.workflow import BaseWorkflow

class process_images_offline_subworkflows_PWF(BaseWorkflow):

	def set_metadata(self):
		self.metadata = {
			"label": "process_images",
		}

	def set_inputs(self):
		self.inputs = {
			"list_of_fits": {
				"type": "file[]",
			},
		}

	def set_outputs(self):
		self.outputs = {
			"before_noise_remover": {
				"type": "file",
				"outputSource": "subworkflow/before_noise_remover",
			},
			"after_noise_remover": {
				"type": "file",
				"outputSource": "subworkflow/after_noise_remover_plot",
			},
			"top_before_noise_remover_plot": {
				"type": "file",
				"outputSource": "imageplotter/output",
			},
			"top_after_noise_remover_plot": {
				"type": "file",
				"outputSource": "after_plot_inspect/output",
			},
		}

	def set_steps(self):
		self.steps = {
			"subworkflow": {
				"in": {
					"fit_list": {
						"source": "list_of_fits",
					},
				},
				"out": [
					"before_noise_remover",
					"after_noise_remover_plot",
				],
				"run": "process_images_offline.py",
				"label": "subworkflow",
			},
			"imageplotter": {
				"in": {
					"input_fits": {
						"source": "list_of_fits",
					},
					"output_image": {
						"default": "top_before_noise_remover.png",
					},
				},
				"out": [
					"output",
				],
				"run": "../steps/imageplotter.py",
				"label": "imageplotter",
			},
			"noiseremover": {
				"in": {
					"input": {
						"source": "list_of_fits",
						"valueFrom": "$(self[0])"
					},
					"output_file_name": {
						"valueFrom": "$('top_no_noise_' + inputs.input[0].basename)"
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
						"source": "noiseremover/output",
						"valueFrom": "$([self])"
					},
					"output_image": {
						"default": "top_after_noise_remover.png",
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
	process_images_offline_subworkflows_PWF()