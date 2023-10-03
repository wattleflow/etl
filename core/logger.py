import os
import sys
import datetime

from etl.core.constants import MSG_MUST_HAVE
from etl.utils.base import WattleUtils

from logging import (getLogger, Logger, Formatter, StreamHandler, DEBUG, INFO, ERROR)
from logging.handlers import TimedRotatingFileHandler

# WattleLogger
class WattleLogger(Logger):
    """
        This class is a wrapper for a python 'logging' based on Logger.

        Constructor:
            config - dict(name, path, level, format)
                name   - logger name
                path   - file path such as logs/name.log
                level  - logging level such as DEBUG, INFO ...
                format - string format with fields 
                         https://docs.python.org/3/library/logging.html#logrecord-attributes
    """
    def __init__(self, config):
        assert isinstance(config, dict)
        assert isinstance(config['name'], str)

        name   = config['name']
        path   = config['path'] if 'path' in config else None
        level  = config['level'] if 'level' in config else DEBUG
        format = config['format'] if 'format' in config else None

        super().__init__(name, level)

        if path:
            if not WattleUtils.path_exists(path):
                raise FileNotFoundError(MSG_MUST_HAVE.format("Correct path"))

        # Formating
        if format is None:
            format='%(asctime)s %(msecs)d - %(levelname)s - %(module)s - %(funcName)s - %(message)s'
        
        formatter = Formatter(format, datefmt='%Y-%m-%d %H:%M:%S')

        # File handler
        if path:
            filelog = TimedRotatingFileHandler(path, when='D', interval=1, backupCount=7)
            filelog.setFormatter(formatter)
            filelog.setLevel(level)
            self.addHandler(filelog)

        # Console handler 
        console = StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        console.setLevel(level)
        self.addHandler(console)
        # log.shutdown(); log.manager._clear_cache()

