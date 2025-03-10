from PWF.src.CommandLineTool import BaseCommandLineTool

class DownloadImages(BaseCommandLineTool):

    def metadata(self):
        self.label = "download_images"

    
    def inputs(self):
        self.inputs_dict = {
            "url_list": {
                "type": "file",
                "prefix": "-i",
                "position": 0
            }
        }


    def outputs(self):
        self.outputs_dict = {
            "output": {
                "type": "file[]",
                "glob": "*.fits"
            }
        }


    # FIXME: Better function name
    def command_line(self):
        self.base_command: str = "wget"
    
    
if __name__ == "__main__":
    DownloadImages(main=True)
    