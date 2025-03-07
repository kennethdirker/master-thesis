from transpile.PWF.CommandLineTool import BaseCommandLineTool

class DownloadImages(BaseCommandLineTool):

    def metadata(self):
        # FIXME User doesn't see self.id, ommit it from them?
        self.id = "download_images"
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


    def base_command(self):
        return "wget"
    
    
if __name__ == "__main__":
    DownloadImages(main=True)
    