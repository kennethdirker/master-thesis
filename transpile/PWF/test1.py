# import importlib
# import inspect
# import sys

# from pathlib import Path

# def f(name):
#     l = name.split("_")
#     l = [t.capitalize() for t in l]
#     return "".join(l)

# path_pyfile = Path('example/steps/download_images.py')
# sys.path.append(str(path_pyfile.parent))
# print(path_pyfile.parent)
# print(path_pyfile.stem)
# print(f(path_pyfile.stem))
# # mysterious = importlib.import_module(f(path_pyfile.stem))
# # mysterious = importlib.import_module(path_pyfile.stem)
# mysterious = importlib.import_module("download_images.py")

# for name_local in dir(mysterious):
#     if inspect.isclass(getattr(mysterious, name_local)):
#         print(f'{name_local} is a class')
#         MysteriousClass = getattr(mysterious, name_local)
#         mysterious_object = MysteriousClass()
# # def _load_single_class_from_file(file_uri: str):
# #     """
    
# #     """
# #     # NOTE: Dynamic class loading from file (hint):
# #     # https://python-forum.io/thread-7923.html
# #     path = Path(file_uri)
# #     if not path.is_file():
# #         raise FileNotFoundError(f"{file_uri} is not a file")
# #     sys.path.append(str(path.parent))
# #     potential_module = importlib.import_module(path.stem)

# #     for class_attr in dir(potential_module):
# #         if inspect.isclass(getattr(potential_module, class_attr)):
# #             return getattr(potential_module, class_attr)

# # obj = _load_single_class_from_file(sys.argv[1])
# # print(type(obj))

from abc import ABC, abstractmethod
class BaseProcess(ABC):
    def __init__(self):
        self.id = None

    # @abstractmethod
    # def metadata(self):
    #     pass