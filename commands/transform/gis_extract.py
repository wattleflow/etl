import os
import logging
import pandas as pd

from etl.core.constants import (
    MSG_FILE_EXIST,
    METHOD_EXEC,
    MSG_CSV_FILE_ERROR,
    MSG_FILE_CREATED
)

from etl.core.concrete import WattleExtract
from etl.core.utils import WattleUtils

class GisInputError(Exception):
    pass

class FileReadingError(Exception):
    pass

class GisExtract(WattleExtract):
    """
      This class is a simple implementation of an latitude and longitude transformer command.
      Using provided `DataFrame` or `params`, the class extract values from a text field 
      using regular expression. Class then creates new fields `latitude` and `longitude`and assigns 
      extracted values. If output file name is given, the result will be saved to the `output` file.

      Constructor(params)
          input     - must be a local file path.(optional if Dataframe is provided).
          output    - local path (if not given, text file will be stored next to the input pdf).
          dataframe - must be Pandas data frame (optional if input is provided).
          columns   - csv field list (optional).
          usecols   - csv field list (optional).
          overwrite - True|False. (optional for files).

      Methods:
          execute(self) - evaluate given input params and extracts archive.
    """
    def __init__(self, log, params):
        super().__init__(log, params)

        self._df      = params['df'] if 'df'           in params else None
        self._input   = params['input'] if 'input'     in params else None
        self._output  = params['output']  if 'output'  in params else None
        self._columns = params['columns'] if 'columns' in params else None
        self._usecols = params['usecols'] if 'usecols' in params else None
        self._overwrite = params['overwrite'] if 'overwrite' in params else True

    def _read(self):
        self.log.debug("{}._read()".format(self.__class__.__name__))
        
        if not os.path.exists(self._input):
            self.log.error(MSG_NOT_FOUND.format("Path", self._input))
            raise FileNotFoundError(MSG_NOT_FOUND.format("Path", self._input))

        try:
            if not self._columns:
                self._df = pd.read_csv(self._input)
            else:
                self._df = pd.read_csv(self._input, usecols=self._columns)
            
            if self._usecols and isinstance(self._usecols, list):
                self._df.columns = self._usecols

        except Exception as e:
            msg = MSG_CSV_FILE_ERROR.format(self._input)
            self.log.error(msg)
            self.log.error(e)
            raise FileReadingError(msg)

        self._read_requested_stats()

        if self._unique and isinstance(self._unique, list):
            self._distinct()

    def _save(self):
        self.log.debug("{}._save()".format(self.__class__.__name__))

        if self._output is None: 
            self.log.info("Output not given.")
            return

        if not WattleUtils.path_exists(self._output):
            msg = MSG_NOT_FOUND.format("Path", self._output)
            self.log.error(msg)
            raise FileExistsError(msg)

        try:
            if not os.path.exists(self._output):
                self._df.to_csv(self._output, header=True, index=False)
                self.log.info(MSG_FILE_CREATED.format(self._output))
            else:
                if self._overwrite: 
                    self._df.to_csv(self._output, header=True, index=False)
                    self.log.info("File overwritten: {}".format(self._output))

        except Exception as e:
            self.log.error(MSG_CREATION_ERROR.format(self._output))

    def _update_gis(self, df, column):
        if not isinstance(df, pd.DataFrame): raise TypeError("Unexpected dataframe!")
        if column in df.columns:
            df = WattleUtils.update_gis(df, column)
        return df

    def extract(self):
        super().extract()

        if self._df:
            self._update_gis()
        else if self._input:
            self._read()
            self._update_gis()
        else:
            raise GisInputError("Unknown input!")

        self._save()
        return self._df
