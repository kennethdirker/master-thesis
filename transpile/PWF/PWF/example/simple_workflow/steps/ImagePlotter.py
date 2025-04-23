from PWF.src.CommandLineTool import BaseCommandLineTool

class ImagePlotter(BaseCommandLineTool):

    def set_metadata(self):
        self.label = "imageplotter"

    
    def set_inputs(self):
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
        self.outputs = {
            "output": {
                "type": "file",
                "glob": "$output_image$"
            }
        }


    # FIXME: Better function name
    def set_base_command(self):
        self.base_command = ["python", "../scripts/imageplotter.py"]
        # NOTE TO SELF: Point to the actual script... so not like this... 
        # !!!self.base_command = ["python", "ImagePlotter.py"]!!!
    
    
if __name__ == "__main__":
    ImagePlotter(main=True)
    