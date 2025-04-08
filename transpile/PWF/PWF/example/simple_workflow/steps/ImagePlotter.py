from PWF.src.CommandLineTool import BaseCommandLineTool

class imagePlotter(BaseCommandLineTool):

    def set_metadata(self):
        self.label = "imageplotter"

    
    def set_inputs(self):
        # FIXME make input ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.inputs = {
            "input_fits": {
                "type": "file[]",
                "position": 0
            },
            "output_image": {
                "type": "string",
                "position": 1
            }
        }


    def set_outputs(self):
        # FIXME make output ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.outputs = {
            "output": {
                "type": "file",
            # TODO Does the following work???
            # NOTE: Prob not at workflow level, as input ids are not unique yet
                "glob": "inputs/output_image"
            }
        }


    # FIXME: Better function name
    def set_base_command(self):
        self.base_command = ["python", "../scripts/imageplotter.py"]
        # NOTE TO SELF: Point to the actual script... so not like this... 
        # !!!self.base_command = ["python", "ImagePlotter.py"]!!!
    
    
if __name__ == "__main__":
    imagePlotter(main=True)
    