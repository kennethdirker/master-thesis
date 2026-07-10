

default = {
    "a": "A",
    "b": "B",
    "c": "C",
}

l = [f'"{k}":{v}' for k, v in default.items()]
l = f'{{{", ".join(l)}}}'
print(default)
print(l)