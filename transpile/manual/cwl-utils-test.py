from pathlib import Path
from ruamel import yaml
import sys

from cwl_utils.parser import load_document_by_uri, save

# File Input - This is the only thing you will need to adjust or take in as an input to your function:
cwl_file = Path("../source/example_tool.cwl")  # or a plain string works as well

# Import CWL Object
cwl_obj = load_document_by_uri(cwl_file)

# View CWL Object
# print("List of object attributes:\n{}".format("\n".join(map(str, dir(cwl_obj)))))


print("List of object attributes:\n{}".format("\n".join(map(str, dir(cwl_obj)))))
o = cwl_obj.requirements
print(type(o), o)
for i in o:
	print(i)
	t = [f"{k}\n" if not k.startswith("_") else "" for k in dir(i)]
	print("".join(map(str, t)))
	break
