from PWF.src.Workflow import BaseWorkflow

class ProcessImages(BaseWorkflow):

    def metadata(self):
        self.label = "process_images"

    
    def inputs(self):
        # FIXME make input ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.inputs_dict = {
            "url_list": {
                "type": "file"
            }
        }


    def outputs(self):
        # FIXME make output ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.outputs_dict = {
            "before_noise_remover": {
                "type": "file",
            # TODO Does the following work???
            # NOTE: Prob not at workflow level, as input ids are not unique yet
                "glob": self.runtime_inputs["output_image"]
            }
        }


    def steps(self):
        # TODO Complete steps
        self.steps = {
            "download_iamges": {
                "in": {
                    "url_list": {
                        "source": "url_list"
                    }
                },
                "out": "output",
                # NOTE how do we call the file???
                "run": "../steps/DownloadImages.py",
                "label": "download_images"
            },
            "imageplotter": {
                "in": {
                    "input_fits": {
                        # NOTE: indicates dir 
                        "source": "download_images/output"
                    },
                    "output_image": {
                        "default": "before_noise_remover.png"
                    }

                },
                "out": "output",
                "run": "../steps/ImagePlotter.py",
                "label": "imageplotter"
            }
        }
    
    
if __name__ == "__main__":
    ProcessImages(main=True)
    