from test2 import CommandLineTool

class Step(CommandLineTool):
    def __init__(self):
        super().__init__()

    # def metadata(self):
        # self.id = "EWAJA WE MOETEN DOOOOOOR"

s = Step()
print(s.id)