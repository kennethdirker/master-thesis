import subprocess
import sys 

print(sys.version)


cmd = [
    "echo", "hello world1", "echo", "hello world2"
]

# Single
res = subprocess.run(cmd, capture_output=True, text=True)
print(res.stdout)

# # Multiple
cmds = [
    ["echo", "hello world3"],
    ["echo", "hello world4"],
]

for cmd in cmds:
    res = subprocess.run(cmd, capture_output=True, text=True)
    print(res.stdout)

