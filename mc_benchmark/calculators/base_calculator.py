from abc import ABC, abstractmethod

class BaseCalculator(ABC):

    @staticmethod
    @abstractmethod
    def pi_calculator():
        raise NotImplementedError

    # @staticmethod
    # @abstractmethod
    # def casino_simulation():
    #     raise NotImplementedError

    # @staticmethod
    # @abstractmethod
    # def elo_calculator():
    #     raise NotImplementedError
