from PWF.src.Workflow import BaseWorkflow

class ProcessImages(BaseWorkflow):

    def set_metadata(self):
        self.label = "process_images"

    
    def set_inputs(self):
        # FIXME make input ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.inputs = {
            "animal_file": {
                "type": "file"
            },
            "bodypart_file": {
                "type": "file"
            }
        }


    def set_outputs(self):
        # FIXME make output ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.outputs = {
            "animal_bodypart": {
                "type": "string",
                "outputSource": "concat_strings/output"
            }
        }


    def set_steps(self):
        # TODO Complete steps
        self.steps = {
            "get_animal": {
                "in": {
                    "file_path": {
                        "source": "animal_file"
                    }
                },
                "out": "output",
                "run": "cat.py",
                "label": "get_animal"
            },
            "get_bodypart": {
                "in": {
                    "file_path": {
                        "source": "bodypart_file"
                    }
                },
                "out": "output",
                "run": "cat.py",
                "label": "get_bodypart"
            },
            "concat_strings": {
                "in": {
                    "left": {
                        "source": "get_animal/output"
                    },
                    "right": {
                        "source": "get_bodypart/output"
                    }
                },
                "out": "output",
                "run": "concat.py",
                "label": "concatenate_strings"
            },

        }
    
    
if __name__ == "__main__":
    ProcessImages(main=True)
    