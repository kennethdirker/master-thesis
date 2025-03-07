import uuid, inspect
from test1 import BaseProcess

class CommandLineTool(BaseProcess):
    def __init__(self):
        super().__init__()
        step_path = inspect.getfile(type(self))
        self.id = f"{step_path}:{uuid.uuid4()}"
        self.metadata()

    def metadata(self):
        self.id = "BaseCommandLineTool"

    