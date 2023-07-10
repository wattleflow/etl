import os
import logging
import datetime
import subprocess

from etl.core.constants import (
    MSG_FILE_EXIST,
    MSG_FILE_OVERWRITEN,
    MSG_FILE_DOWNLOADED,
    METHOD_EXEC,
    MSG_NOT_FOUND,
)

from etl.core.concrete import WattleExtract
from etl.utils.base import WattleUtils

class Download(WattleExtract):
    """
      This class is a simple implementation of an concreate Download extractor command. 
      THe functionality is downloading file from a given url localy in a given `output` path.

      Constructor(params)
          input     - must be a local file path.
          output    - local path (crates dir if doesn't exists.)
          overwrite - True|False.

      Methods:
          execute(self)
    """
    def __init__(self, log, params):
        super().__init__(log, params)
        assert isinstance(params['url'], str)
        assert isinstance(params['local'], str)

        self.url = params['url']
        self.local = params['local']
        self.overwrite = params['overwrite'] if 'overwrite' in params else True

    def _download(self):
        self.log.debug( "{}._download()".format(self.__class__.__name__))

        if os.path.exists(self.local):
            if not self.overwrite:
                self.log.info(MSG_FILE_EXIST.format(self.local)); return 0
            else:
                self.log.info(MSG_FILE_OVERWRITEN.format(self.local)); return 1         

        with open(self.local, "w") as f:
            result = subprocess.call(["curl", self.url], stdout=f)

        self.log.info(MSG_FILE_DOWNLOADED.format(self.local))
        return result

    def extract(self):
        super().extract()
        try:
            # path = os.path.split( os.path.abspath( self._output ))[0]
            # LOGGER.debug( f"Path: {path}" )
            if not WattleUtils.path_exists(self.local):
                msg = MSG_NOT_FOUND.format("Path", self.local)
                self.log.error(msg)
                raise FileNotFoundError(msg)
            return self._download()
        except FileNotFoundError as e:
            self.log.info( e )
