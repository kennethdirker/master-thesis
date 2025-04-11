from test3 import BaseProcess

class CommandLineTool(BaseProcess):
    def __init__(self):
        super().__init__()
        self.metadata()

    def metadata(self):
        self.id = "BaseCommandLineTool"

    