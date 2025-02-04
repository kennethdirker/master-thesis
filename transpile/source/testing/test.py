
baseCommand = [
    "echo",
    "hello"
]

# inputs = {"<who>": "Kenneth"}

# argument = "<who>"
# baseCommand.append(argument)
# t = ' '.join(baseCommand)
# for k, v in inputs.items():
#     t = t.replace(k, v)
# print(t)
print([*baseCommand, "world"])