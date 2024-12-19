import subprocess
import sys 
import dask
import dask.delayed
from typing import List

print(sys.version)




class Step:
    def __init__(self, cmd: List[str]):
        self.cmd = cmd



@dask.delayed
def wrapper(steps):
    for step in steps:
        cmd = step.cmd
        res = subprocess.run(cmd, capture_output=True, text=True)
        print(res.stdout)
    return 0


steps = []
steps.append(Step(["echo", "Hello user! `ls -l` returns:"]))
steps.append(Step(["ls", "-l"]))

process = dask.delayed(wrapper)(steps)
res = process.compute()
print(res)