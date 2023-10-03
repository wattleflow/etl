import os
import logging
import pandas as pd

from etl.core.constants import (
    MSG_FILE_EXIST,
    MSG_FILE_DOWNLOADED,
    METHOD_EXEC,
    MSG_CSV_FILE_ERROR,
    MSG_FILE_CREATED
)

from etl.core.concrete import WattleExtract
from etl.utils.base import WattleUtils

class FilePathError(Exception):
    pass

class FileReadingError(Exception):
    pass

class CsvReader(WattleExtract):
    """
      This class is a simple implementation of an concreate CSV extractor reader command.
      Using given `params` and pandas DataFrame, class is an example of how to manipulate 
      loaded csv data columns and log some basic information. The result is saved to a `output` 
      file and DataFrame returned by `execute` method.

      Constructor(params)
          input     - must be a local file path.
          output    - local path (if not given, text file will be stored next to the input pdf).
          columns   - csv field list.
          renamed   - new column name list.
          unique    - produce unique values for a given column list.
          min-maxm  - create log entries for min/max values of a given column list.
          overwrite - True|False.

      Methods:
          execute(self) - evaluate given input params and extracts archive.
    """
    def __init__(self, log, params):
        super().__init__(log, params)
        assert isinstance(params['input'], str) 

        self._df      = None
        self._input   = params['input']
        self._output  = params['output']  if 'output'  in params else None
        self._columns = params['columns'] if 'columns' in params else None
        self._renamed = params['renamed'] if 'renamed' in params else None
        self._unique  = params['unique']  if 'unique'  in params else None
        self._min_max = params['min-max'] if 'min'     in params else None
        self._overwrite = params['overwrite'] if 'overwrite' in params else True

    def _distinct(self):
        self.log.debug("{}._distinct()".format(self.__class__.__name__))
        try:
            for column in self._unique:
                distinct = sorted(self._df[column].apply(lambda x: x.strip() if isinstance(x, str) else x).dropna().unique())
                self.log.info("({}) {}: {}".format(len(distinct), column, distinct))
        except Exception as e:
            self.log.error(e)

    def _read_requested_stats(self):
        self.log.debug("{}._read_requested_stats()".format(self.__class__.__name__))

        self.log.info("Columns:({})[{}]".format(len(self._df.columns), ','.join(self._df.columns)))
        self.log.info("Records: {}".format(len(self._df)))
       
        # Min/Max values for each given column
        if self._min_max:
            for each in self._min_max:
                self.log.info("{} min: {} max: {}".format(
                    self._df[each].min(),
                    self._df[each].max()
                    )
                )

    def _read(self):
        self.log.debug("{}._read()".format(self.__class__.__name__))

        try:
            if not self._columns:
                self._df = pd.read_csv(self._input)
            else:
                self._df = pd.read_csv(self._input, usecols=self._columns)
            
            if self._renamed and isinstance(self._renamed, list):
                self._df.columns = self._renamed

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
            raise FilePathError(msg)

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

    def extract(self):
        super().extract()

        if not WattleUtils.path_exists(self._input):
            msg = MSG_NOT_FOUND.format("File", self._input)
            self.log.error(msg)
            raise FileNotFoundError(msg)

        self._read()
        self._save()
        return self._df
