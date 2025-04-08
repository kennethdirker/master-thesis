from PWF.src.CommandLineTool import BaseCommandLineTool

class NoiseRemover(BaseCommandLineTool):

    def set_metadata(self):
        self.label = "noiseremover"

    
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
        # FIXME make output ids globally unique to ensure sub-processes from
        # overwriting each other. Use process id?
        self.outputs = {
            "output": {
                "type": "file",
            # TODO Does the following work???
            # NOTE: Prob not at workflow level, as input ids are not unique yet
                "glob": self.runtime_inputs["output_file_name"]
            }
        }


    # FIXME: Better function name
    def set_base_command(self):
        self.base_command = "python noiseremover.py"
    
    
if __name__ == "__main__":
    NoiseRemover(main=True)
    