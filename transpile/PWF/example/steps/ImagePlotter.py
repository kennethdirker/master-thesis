from transpile.PWF.CommandLineTool import BaseCommandLineTool

class imagePlotter(BaseCommandLineTool):

    def metadata(self):
        # FIXME User doesn't see self.id, ommit it from them?
        self.id = "imageplotter"
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
                "glob": self.runtime_inputs["output_image"]
            }
        }


    def command_line(self):
        self.base_command = "python imageplotter.py"
    
    
if __name__ == "__main__":
    imagePlotter(main=True)
    