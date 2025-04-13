from PWF.src.CommandLineTool import BaseCommandLineTool

class NoiseRemover(BaseCommandLineTool):

    def set_metadata(self):
        self.label = "imageplotter"

    
    def set_inputs(self):
        # FIXME make input ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.inputs = {
            "input": {
                "type": "file",
                "position": 0
            },
            "output_file_name": {
                "type": "string",
                "position": 1
            }
        }


    def set_outputs(self):
        self.outputs = {
            "output": {
                "type": "file",
            # TODO Does the following work???
            # NOTE: Prob not at workflow level, as input ids are not unique yet
                "glob": "$output_file_name$"
            }
        }


    # FIXME: Better function name
    def set_base_command(self):
        self.base_command = ["python", "../scripts/noiseremover.py"]
        # NOTE TO SELF: Point to the actual script... so not like this... 
        # !!!self.base_command = ["python", "ImagePlotter.py"]!!!
    
    
if __name__ == "__main__":
    NoiseRemover(main=True)
    