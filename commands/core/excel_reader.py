mport os
import sys
import csv
import xlrd
import numpy as np
import pandas as pd

from etl.core.constants import (
    MSG_FILE_EXIST,
    METHOD_EXEC,
    MSG_NOT_FOUND,
    MSG_REC_COUNT,
    MSG_FILE_SAVED,
    MSG_FILE_OVERWRITEN,
    MSG_FILE_DOWNLOADED
)

from etl.core.concrete import WattleExtract
from etl.core.utils import WattleUtils

class ExcelReaderFileError(Exception):
    pass

class ExcelReader(WattleExtract):
    """
      This class is a simple implementation of an Excel reader Command.
      The class can uses excel file to load data into into a dataframe.

      Constructor(params)
          input        - must be a local file path.
          output       - local path (if not given, text file will be stored next to the input pdf).
          columns      - columns select/filter columns to be loaded from a csv.
          sheet-name   - excel sheet name.
          skip-rows    - rows to skip when loading file.
          skip-footer  - footer rows to skip when loading file. 
          verbose      - print verbose informatin.

      Methods:
          execute(self)  - evaluate given input params and extracts archive.

      Regex patterns: 
          patterns = [
            '([A-Z]+) (ACT|NSW|NT|SA|QLD|TAS|VIC|WA) ([0-9]+)',
            '([A-Z]+) (ACT|NSW|NT|SA|QLD|TAS|VIC|WA)  ([0-9]+)',
            '([A-Z]+)  (ACT|NSW|NT|SA|QLD|TAS|VIC|WA)  ([0-9]+)',
            '([A-Z]+) ([A-Z]+) (ACT|NSW|NT|SA|QLD|TAS|VIC|WA) ([0-9]+)', 
            '([A-Z]+) ([A-Z]+)  (ACT|NSW|NT|SA|QLD|TAS|VIC|WA)  ([0-9]+)', 
            '([A-Z]+)  ([A-Z]+)  (ACT|NSW|NT|SA|QLD|TAS|VIC|WA)  ([0-9]+)', 
          ]
    """
    def __init__(self, log, params):
        super().__init__(log, params)

        assert isinstance(params['input'], str)
        assert isinstance(params['output'], str)

        self._input     = params['input']     if 'input'     in params else None
        self._local     = params['local']     if 'local'     in params else None
        self._names     = params['renamed']   if 'renamed'   in params else None
        self._keys      = params['keys']      if 'keys'      in params else None
        self._overwrite = params['overwrite'] if 'overwrite' in params else None
        self._kwargs    = {}

        if 'columns'     in params: self._kwargs['usecols']     = params['columns'] 
        if 'sheet-name'  in params: self._kwargs['sheet_name']  = params['sheet-name']
        if 'columns'     in params: self._kwargs['names']       = params['columns']
        if 'skip-rows'   in params: self._kwargs['skiprows']    = params['skip-rows']
        if 'skip-footer' in params: self._kwargs['skipfooter']  = params['skip-footer']
        if 'verbose'     in params: self._kwargs['verbose']     = params['verbose']
        
        self.yes2int   = lambda x : 1 if x == 'Y' else 0
        self.minus2int = lambda x : 0 if x == '-' else x
        self.str2int   = lambda x : int(x) if str(x).isdigit() else 0
        # self.strtoint  = lambda x : int(x) if str(x).replace('-','',1).isdigit()
        self._df = None

    def _read(self):
        self.log.debug("{}._read()".format(self.__class__.__name__))

        if self._input is None: 
            msg = MSG_NOT_FOUND.format("File", self._input)
            self.log.error( msg )
            raise FileNotFoundError( msg )

        try:
            self._df = pd.read_excel( self._input, **self._kwargs )
            if self._names: self._df.columns = self._names
            if self._keys: self._df.sort_values(self._keys, ascending=True, inplace=True)
 
            if isinstance(self._df, pd.DataFrame) : 
                self.log.info("Column count: {}".format(len(self._df.columns)))
                self.log.info(MSG_REC_COUNT.format(len(self._df)))

        except Exception as e:
            msg = "Unknown error reading the Excel file: {}".format(self._input)
            self.log.error( msg )
            self.log.error( e )
            raise ExcelReaderFileError( msg )

    def _save(self):
        self.log.debug("{}._save()".format(self.__class__.__name__))

        if self._local is None: 
            self.log.info("Output not given.")
            return

        # path = path if path.endswith('/') else f'{path}/'
        if not isinstance(self._df, pd.DataFrame):
            msg = "DataFrame is not alocated."
            self.log.error( msg )
            raise TypeError(msg)

        try:
            if os.path.exists( self._local ) or self._overwrite:
                self._df.to_csv(
                    path_or_buf = self._local,
                    index       = False, 
                    encoding    = 'utf-8'
                    # **kwargs
                )
                self.log.info( MSG_FILE_SAVED.format(self._local) )
        except IOError as e:
            msg = f"Saving data to a csv {self._ouput}.\n {e}"
            self.log.error( msg )
            raise IOError( msg )

        except ValueError as e:
            msg = f"Saving index data a file {self._local}.\n {e}"
            self.log.error( msg )
            raise ValueError( msg )

    def extract(self):
        super().extract()

        if (not os.path.exists(self._input)) :
            raise FileExistsError (MSG_NOT_FOUND.format("File", self._input))
        
        self._read()
        self._save()
        return self._df
