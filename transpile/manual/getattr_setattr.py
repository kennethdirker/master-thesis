class test:
    def __init__(self):
        self.obj1 = None
        self.obj2 = None
        self.obj3 = None
        self.obj4 = None
        self.obj5 = None
        self.obj6 = None
        self.obj7 = None
        self.obj8 = None

    def __init__(self, cwl_object: object):
        if cwl_object:
            attrs = dir(cwl_object)
            for attr in attrs:
                if not "__" in attr:
                    self.__setattr__(attr, getattr(cwl_object, attr))

    def func(self, d: dict):
        for key, value in d.items():
            setattr(self, key, value)

    def get(self):
        s = ""
        for i in range(1, 9):
            s += f"{getattr(self, f'obj{i}')}"
        return s

# Driver code
d = {
    "obj1" : 1,
    "obj2" : 2,
    "obj3" : 3,
    "obj4" : 4,
    "obj5" : 5,
    "obj6" : 6,
    "obj7" : 7,
    "obj8" : 8
}

o = test()
o.func(d)
print(o.get())
