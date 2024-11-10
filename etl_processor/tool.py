"""
The abstract class Tool that defines the structure of an ETL tool.

An ETL tool is a class that implements the following methods:
    - run: It contains the main logic of the tool to either extract, transform or load data.
"""

from abc import ABC, abstractmethod


class Tool(ABC):
    """
    A ETL tool implements all the necessary routines and methods to either extract, transform or load data.

    An ETL tool is a class that implements the following methods:
        - run: It contains the main logic of the tool to either extract, transform or load data.
    """

    @abstractmethod
    def run(self) -> None:
        """
        Implement the main logic of the tool to either extract, transform or load data.

        Raises
        ------
        ETLException
            If an error occurs during the execution of the tool.
        """
        pass

    @abstractmethod
    async def arun(self) -> None:
        """
        Implement the main logic of the tool to either extract, transform or load data asynchronously.

        Raises
        ------
        ETLException
            If an error occurs during the execution of the tool.
        """
        pass
