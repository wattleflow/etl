import os
import re
import pandas as pd

from etl.core.constants import (
    METHOD_EXEC,
    MSG_FILE_SAVED,
    MSG_TXT_LENGTH,
    MSG_REC_COUNT
)

from etl.core.concrete import WattleExtract
from etl.core.utils import WattleUtils

class ReadingTxtfFileError(Exception):
    pass

class TextReader(WattleExtract):
    """
      This class is a simple implementation of an text extractor using regex patterns.
      The class can uses text file input and `regex` tokens to extract specific text patterns.
      Class use pandas DataFrame to store extracted data to an `ouptut` csv file.

      Constructor(params)
          input    - must be a local file path.
          output   - local path (if not given, text file will be stored next to the input pdf).
          columns  - columns select/filter columns to be loaded from a csv.
          pages    - user given page range.
          patterns - `regex` tokens (they must be fited for a JSON format).
          index    - is given index to sort out result.

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

        assert isinstance(params['input'],    str) 
        assert isinstance(params['output'],   str)
        assert isinstance(params['columns'],  dict)
        assert isinstance(params['keys'],     list)
        assert isinstance(params['patterns'], dict)

        self._input     = params['input']     if 'input'     in params else None
        self._output    = params['output']    if 'output'    in params else None
        self._overwrite = params['overwrite'] if 'overwrite' in params else None
        self._columns   = params['columns']  if 'columns'    in params else None
        self._keys      = params['keys']     if 'keys'       in params else None
        self._tokens    = params['patterns'] if 'patterns'   in params else None
        self._df        = None

    def _read(self):
        self.log.debug( "_read: {}".format(self._input) )

        if not os.path.exists(self._input):
            msg = MSG_NOT_FOUND.format("File", self._input)
            self.log.critical(msg)
            raise FileNotFoundError(msg)
        try:
            text = ""
            with open(self._input, 'r', encoding='utf-8') as f:
                text = f.read()

            self.log.info(MSG_TXT_LENGTH.format(len(text)))
            self.log.debug("columns.keys: {}".format(self._columns.keys()))
            self.log.debug("columns.values: {}".format(self._columns.values()))

            remapped = sorted(map(lambda c: (c[1], c[0]), enumerate(self._columns.keys())))
            self.log.debug( "Remapped : {}".format(remapped) )
            self.log.debug( "Tokens: {}".format(self._tokens) )

            data = []
            matched = WattleUtils.extract_patterns(text, self._tokens)
            for m in matched:
                tokens = m[1]
                for rec in tokens:
                    newrec = {}
                    for o in remapped:
                        newrec.update( { o[0]: rec[o[1]] } )
                    data.append( newrec )

            self.log.info( MSG_REC_COUNT.format(len(data)) )

            byindex  = [ rec[0] for rec in remapped ]
            self.log.debug( "Byindex : {}".format(byindex) )

            self._df = pd.DataFrame(data, columns=byindex)
            self.log.info("DataFrame loaded: {}".format( len(self._df) ))

            self._df.sort_values(self._keys, ascending=True, inplace=True) 
            self._df.reset_index(inplace=True, drop=True)

            self.log.info(MSG_REC_COUNT.format(len(self._df)))
    
        except Exception as e:
            msg = "Error reading PDF: {}".format(self._input)
            self.log.error(msg)
            self.log.error(e)
            raise ReadingTxtfFileError(msg)

    def _save(self):
        self.log.debug("{}._save()".format(self.__class__.__name__))
        if not WattleUtils.path_exists(self._output):
            msg = MSG_NOT_FOUND.format("Path", self._output)
            self.log.error(msg)
            raise FileNotFoundError(msg)

        if not isinstance(self._df, pd.DataFrame):
            msg = "DataFrame not allocated"
            self.log.error(msg)
            raise ReadingTxtfFileError(msg) 

        if WattleUtils.file_exists(self._output):
            if self._overwrite:
                self._df.to_csv(self._output, index=False)
                self.log.info("File overwriten: {}".format(self._output)) 
            else:
                self.log.info(MSG_FILE_EXIST.format(self._output))
        else:
            self._df.to_csv(self._output, index=False)
            self.log.info(MSG_FILE_SAVED.format(self._output))       

    def extract(self):
        self.log.debug( "{}.extract()".format(self.__class__.__name__))
        self._read()
        self._save()
        return self._df
