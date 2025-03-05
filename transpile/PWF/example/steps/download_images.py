import sys
from PWF.command_line_tool import BaseCommandLineTool

class DownloadImages(BaseCommandLineTool):

    def metadata(self):
        self.id = "download_images"
        self.label = "download_images"

    
    def inputs(self):
        self.inputs_dict = {
            "url_list": {
                "type": "file",
                "prefix": "-i"
            }
        }


    def outputs(self):
        self.outputs_dict = {
            "output": {
                "type": "file[]",
                "glob": "*.fits"
            }
        }


    def base_command(self):
        return "wget"
    
    
if __name__ == "__main__":
    tool = DownloadImages(sys.argv[1])
    tool.execute()
    