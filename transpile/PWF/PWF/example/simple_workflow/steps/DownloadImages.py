from PWF.src.CommandLineTool import BaseCommandLineTool

class DownloadImages(BaseCommandLineTool):

    def set_metadata(self):
        self.label = "download_images"

    
    def set_inputs(self):
        self.inputs = {
            "url_list": {
                "type": "file",
                "prefix": "-i",
                "position": 0
            }
        }


    def set_outputs(self):
        self.outputs = {
            "output": {
                "type": "file[]",
                "glob": "*.fits"
            }
        }


    # FIXME: Better function name
    def set_base_command(self):
        self.base_command: str | list[str] = "wget"
    
    
if __name__ == "__main__":
    DownloadImages(main=True)
    