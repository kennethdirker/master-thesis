import subprocess
import sys 

print(sys.version)

# Single
# Doesn't work
cmd = [
    "echo", "hello world1;", "echo", "hello world2"
]
res = subprocess.run(cmd, capture_output=True, text=True)
print(res.stdout)

# Does work
cmd = [
    "echo 'hello world3'; echo 'hello world4'"
]
# Single
res = subprocess.run(cmd, capture_output=True, text=True, shell=True)
print(res.stdout)

# # Multiple
cmds = [
    ["echo", "hello world5"],
    ["echo", "hello world6"],
]

for cmd in cmds:
    res = subprocess.run(cmd, capture_output=True, text=True)
    print(res.stdout)

# # Multiple
cmds = [
    ["echo 'hello world7'; echo 'hello world8'"],
    ["echo 'hello world9'; echo 'hello world10'"]
]
for cmd in cmds:
    res = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    print(res.stdout)

