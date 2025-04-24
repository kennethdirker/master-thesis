from PWF.src.commandlinetool import BaseCommandLineTool

class Concat(BaseCommandLineTool):

    def set_metadata(self):
        self.label = "concat"
        self.doc = "Concatenate 2 strings"

    
    def set_inputs(self):
        self.inputs = {
            "left": {
                "type": "string",
                "position": 0
            },
            "right": {
                "type": "string",
                "position": 1
            }
        }


    def set_outputs(self):
        self.outputs = {
            "output": {
                # "type": "file",
                # "glob": "concat.out"
                "type": "string" 
            }
        }


    # FIXME: Better function name
    def set_base_command(self):
        self.base_command: str | list[str] = ["echo"]
    
    
if __name__ == "__main__":
    Concat(main=True)
    