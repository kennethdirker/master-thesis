import types
from typings import Any
from abc import ABC


class Step(ABC):
    def __init__(self, cwl_object: object = None) -> None: 
        # Generated during convertion from CWL to Python
        # TODO All cwl_utils cwl_object should be here! 
        # TODO Check cwl_utils source code
        if cwl_object:
            attrs = dir(cwl_object)
            for attr in attrs:
                if not "__" in attr:
                    self.__setattr__(attr, getattr(cwl_object, attr))

        # self.cwlVersion = None
        # self.id = None
        # self.baseCommand = None
        # self.arguments = None
        # self.inputs = None
        # self.outputs = None

        # self.attrs = None
        # self.class_ = None
        # self.doc = None
        # self.extension_fields = None
        # self.fromDoc = None
        # self.hints = None
        # self.intent = None
        # self.label = None
        # self.loadingOptions = None
        # self.permanentFailCodes = None
        # self.requirements = None
        # self.save = None
        # self.stderr = None
        # self.stdin = None
        # self.stdout = None
        # self.successCodes = None
        # self.temporaryFailCodes = None


    # def script(self, args: dict[str, str]):
        # """
        # """
        
    
    def run_step(self, args: dict[str, str]) -> Any:
        """
        Execute the step .
        """
        if not self.valid():
            # TODO Check if this is the right way of doing this
            # TODO Get filename of subclass
            raise Exception(f"{self.id} is not a valid step, aborting.")
            # raise Exception(f"File {file} is not a valid step, aborting.")

        # TODO Check input
        # TODO Containers
        # TODO Couple inputs
        # 

        return
    
    def get_step(self) -> function:
        pass



    def valid(self) -> bool:
        """ 
        Verifies whether the step configuration is valid and runnable.

        Returns:
            Boolean: True if valid, False otherwise.
        """
        return True