from pathlib import Path
import sys
# from ruamel.yaml import yaml
from cwl_utils.parser import load_document_by_uri, save
from typing import Union

from Utils import NoneType, getattr_or_nonetype

# File Input - This is the only thing you will need to adjust or take in as an input to your function:
# cwl_file = Path("../../cwl_examples/steps/test_cmd_tool.cwl")  # or a plain string works as well
cwl_file = Path("../../cwl_examples/workflows/test_workflow.cwl")  # or a plain string works as well

# Import CWL Object
cwl_obj = load_document_by_uri(cwl_file)
print(cwl_obj.attrs)

# kwargs: dict[str, Union[str, NoneType]] = {
#     attr: getattr_or_nonetype(cwl_obj, attr) for attr in cwl_obj.attrs
# }
kwargs = {}
for attr in cwl_obj.attrs:
    attr = "class_" if "class" in attr else attr
    print(attr, getattr_or_nonetype(cwl_obj, attr))
    # if "class" in attr:

# def foo(**kwargs):
#     for k, v in kwargs.items():
#         print(k, v)

# foo(**kwargs)

# View CWL Object
# print("List of object attributes:\n{}".format("\n".join(map(str, dir(cwl_obj)))))


# print("List of object attributes:\n{}".format("\n".join(map(str, dir(cwl_obj)))))
# o = cwl_obj.requirements
# print(type(o), o)
# for i in o:
# 	print(i)
# 	t = [f"{k}\n" if not k.startswith("_") else "" for k in dir(i)]
# 	print("".join(map(str, t)))
# 	break
