from yaml import safe_load
with open("test.yaml") as f:
    o = safe_load(f)
    print(o)
    for k, v in o.items():
        print(type(v), v)