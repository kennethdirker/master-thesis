from PWF.src.CommandLineTool import BaseCommandLineTool

class imagePlotter(BaseCommandLineTool):

    def metadata(self):
        self.label = "imageplotter"

    
    def inputs(self):
        # FIXME make input ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.inputs_dict = {
            "input_fits": {
                "type": "file[]",
                "position": 0
            },
            "output_image": {
                "type": "string",
                "position": 1
            }
        }


    def outputs(self):
        # FIXME make output ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.outputs_dict = {
            "output": {
                "type": "file",
            # TODO Does the following work???
            # NOTE: Prob not at workflow level, as input ids are not unique yet
                "glob": self.runtime_inputs["output_image"]
            }
        }


    # FIXME: Better function name
    def command_line(self):
        self.base_command = ["python", "ImagePlotter.py"]
    
    
if __name__ == "__main__":
    imagePlotter(main=True)
    