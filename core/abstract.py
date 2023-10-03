from abc import (
    ABC, 
    abstractproperty, 
    abstractmethod, 
    abstractclassmethod
)

# Asbtract Interface
class WattleFlow(ABC):
    """
        Base class in a WattleFlow framework.
        Must have a __init__ method.
    """
    @abstractmethod
    def __init__(self, **kwargs):
        pass

# Reciever
class WattleReciever(WattleFlow):
    @abstractmethod
    def action(self, action):
        pass

# Command
class WattleCommand(WattleFlow):
    """
        WattleCommand is an apstract generic interface for all ETL commands. 
        The classes inherited from this interface must have "execute" method.

        While constructor methods can be diferent for each class, it is recomened
        that they follow the rules implemented in concrete clases for each ETL,
        process. Remember, keep it as simple as possible.
    """
    @abstractclassmethod
    def execute(self):
        pass

# Composite
class WattleComposite(WattleFlow):
    """
        WeattleComposite represents abstract interface for all ETL processes.
        This interface implements standard composite methods, add, remove 
        and execute. 
    """
    @abstractclassmethod
    def add(self, command):
        pass
    @abstractclassmethod
    def remove(self, command):
        pass
    @abstractclassmethod
    def execute(self):
        pass

# Entity Factry
class WattleFactory(WattleFlow):
    """
        This abstract interface is used for ETL process implementation.
        It garantees the standard ETL approach for all abstract classes. 
    """
    @abstractclassmethod
    def create_extractor():
        pass
    @abstractclassmethod
    def create_transformer():
        pass
    @abstractclassmethod
    def create_loader():
        pass

class WattleVisitor(WattleFlow):
    pass
