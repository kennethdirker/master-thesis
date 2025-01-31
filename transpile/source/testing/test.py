from enum import Enum
from subprocess import run

class Attr(Enum):
    id = 1
    baseCommand = 2
    inputs = 3
    outputs = 4

class Step:
    def __init__(self, o):
        self.attrs = {}
        for attr, value in o.vars():
            if attr in (Attr):
                self.attrs(attr, value)
                setattr(self, attr, value)


    def exec(self):
        line = self.baseCommand
        for key, value in self.inputs:
            line = line.replace(key, value)
        run(line)

class Wrapper:
    pass

o = Wrapper()
o.id = 69
o.baseCommand = "echo '<word>'"
o.inputs = {"<word>": "Hello world!"}
o.trash = "ABABABABA"
# setattr(o, "id", 69)
# setattr(o, "baseCommand", "echo '<word>'")
# setattr(o, "inputs", {"<word>": "Hello world!"})
# setattr(o, Attr.blah, {"<word>": "Hello world!"})
print(o.__dict__)
# step = Step(o)
# print(step.attrs)
# o.exec()