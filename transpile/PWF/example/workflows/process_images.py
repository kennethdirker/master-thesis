import sys
from PWF.workflow import BaseWorkflow

class ProcessImages(BaseWorkflow):

    def metadata(self):
        self.id = "process_images"
        self.label = "process_images"

    
    def inputs(self):
        self.inputs_dict = {
            "url_list": {
                "type": "file",
            }
        }


    def outputs(self):
        self.outputs_dict = {
            "output": {
                "type": "file[]",
                "glob": "*.fits"
            }
        }


    def steps(self):
        self.steps_dict = {
            
        }
        
    
if __name__ == "__main__":
    input_yaml_uri = sys.argv[1]
    tool = ProcessImages(input_yaml_uri)
    tool.execute()
    