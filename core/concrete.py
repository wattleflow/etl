import os
import sys

from abc import (
    abstractmethod
)

from etl.core.logger import WattleLogger
from etl.core.abstract import (
    WattleCommand, 
    WattleComposite,
    WattleFactory
)

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class WattleTask(WattleCommand):
    """
      This class is a simple process task.
        :log - WattleLoger instance
        :param - provide dict parameters from yaml file
        :param["owner"] - if assigned otherwire None
    """
    def __init__(self, log, params):
        assert isinstance(log, WattleLogger)
        assert isinstance(params, dict)

        self.log = log
        self.params = params
        self.owner  = params['owner'] if 'owner' in params else None
        self.log.debug("{}.__init__({},{})".format(self.__class__.__name__, log, params))

    @abstractmethod
    def execute(self):
        self.log.debug("{}.execute()".format(self.__class__.__name__))
        pass

class WattleExtract(WattleTask):
    def execute(self):
        self.log.debug("{}.execute()".format(self.__class__.__name__))
        return self.extract()
   
    @abstractmethod
    def extract(self):
        self.log.debug("{}.extract()".format(self.__class__.__name__))
        pass

class WattleTransform(WattleTask):
    def execute(self):
        self.log.debug("{}.execute()".format(self.__class__.__name__))
        return self.transform()
   
    @abstractmethod
    def transform(self):
        self.log.debug("{}.transform()".format(self.__class__.__name__))
        pass

class WattleLoad(WattleTask):
    def execute(self):
        self.log.debug("{}.execute()".format(self.__class__.__name__))
        return self.load()
   
    @abstractmethod
    def load(self):
        self.log.debug("{}.load()".format(self.__class__.__name__))
        pass

# Process
class WattleProcess(WattleComposite):
    """     
    This class implements Composite interface methods to enable customisation  
    for each ETL process.

    The process should be capable reading commands from YAML file and creating
    comand objects for execution. 

    This approach allows flexibility when implementing different approach in
    creating processes. They can also be synchronous and asynchronous.

    The class is derived from an AbstractComposite class and streamlines 
    the process of reading commands from an external resource.

    Methods:
        Constructor(owner)
        add(command)    - add commands to a list.
        remove(command) - remove command from the list.
        execute()       - execute all commands.
    """
    def __init__(self, owner):
        assert isinstance(owner, WattleFactory)
        assert isinstance(owner.log, WattleLogger)
        assert isinstance(owner.params, dict)

        self.owner = owner
        self.params = owner.params
        self.log = owner.log
        self.log.debug("{}.__init__({})".format(self.__class__.__name__, owner))
        self.commands = []
        self.read_tasks()

    def add(self, command):
        self.log.debug( "{}.add({})".format(self.__class__.__name__, command) )
        self.commands.append(command)

    def remove(self, command):
        self.log.debug( "{}.remove({})".format(self.__class__.__name__, command) )
        self.commands.remove(command)

    def execute(self):
        self.log.debug( "{}.execute()".format(self.__class__.__name__) )
        for command in self.commands:
            command.execute()

    @abstractmethod
    def read_tasks(self):
        self.log.debug( "{}.read_tasks()".format(self.__class__.__name__) )
        assert isinstance(self.owner, WattleFactory)
        assert isinstance(self.owner.log, WattleLogger)
        assert isinstance(self.owner.params, dict)
        pass
