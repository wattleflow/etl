import io
import os
import requests
import zipfile
import pandas as pd

from etl.core.constants import (
    METHOD_EXEC,
    MSG_FOUND,
    MSG_NOT_FOUND,
    MSG_CREATION_ERROR
)

from etl.core.concrete import WattleExtract
from etl.core.utils import WattleUtils

class Unzip(WattleExtract):
    """
      This class is a simple implementation of an concreate Unzip extractor command. 
      The functionality is unpacking a zip archieve into a given `output` path.

      Constructor(params)
          input     - must be a local file path.
          output    - local path (crates dir if doesn't exists.)
          overwrite - True|False.

      Methods:
          execute(self)       - evaluate given input params and extracts archive.
    """
    def __init__(self, log, params):
        super().__init__(log, params)

        assert isinstance(params['input'], str)
        assert isinstance(params['output'], str)
        assert isinstance(params['overwrite'], bool)

        self._input     = params['input']
        self._output    = params['output']
        self._overwrite = params['overwrite']

    def extract(self):
        super().extract()

        if not os.path.exists(self._input): 
            raise FileNotFoundError(MSG_NOT_FOUND.format("Input file", self._input))

        if not WattleUtils.make_dir(self._output):
            raise FileNotFoundError(MSG_CREATION_ERROR.format("Directory", self._output))

        self._zip, n = zipfile.ZipFile(self._input), 0

        for name in self._zip.namelist():
            self.log.info(MSG_FOUND.format("Archieve file", name))
            n += 1

        self._zip.extractall(self._output)
        self.log.info("{} file(s) extracted to: {}.".format(n, self._output))
