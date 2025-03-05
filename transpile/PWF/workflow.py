from abc import ABC, abstractmethod
from .process import BaseProcess

class BaseWorkflow(BaseProcess):

    @abstractmethod
    def steps(self):
        """ Defines the sub-processes of this workflow"""
        # Example:
        # 
        # 
        # 
        pass

    def execute(self) -> None:
        """ Execute the workflow. Can be overwritten to alter execution behaviour. """
        raise NotImplementedError