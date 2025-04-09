from PWF.src.CommandLineTool import BaseCommandLineTool

class Cat(BaseCommandLineTool):

    def set_metadata(self):
        self.label = "cat"
        self.doc = "Read the file"
    
    def set_inputs(self):
        self.inputs = {
            "file_path": {
                "type": "file"
            }
        }


    def set_outputs(self):
        self.outputs = {
            "output": {
                "type": "str"
            }
        }


    # FIXME: Better function name
    def set_base_command(self):
        self.base_command: str | list[str] = "cat"
    
    
if __name__ == "__main__":
    Cat(main=True)
    