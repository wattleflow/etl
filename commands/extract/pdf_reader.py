import io
import os
import re
import sys
import pypdf
import logging

from etl.core.constants import (
    MSG_FILE_EXIST,
    MSG_FILE_OVERWRITEN,
    MSG_FILE_DOWNLOADED,
    METHOD_EXEC,
    MSG_NOT_FOUND,
    MSG_FILE_SAVED,
)

from etl.core.concrete import WattleExtract
from etl.core.utils import WattleUtils

class ReadingPdfFileError(Exception):
    pass

# PDF Reader
class PdfReader(WattleExtract):
    """
      This class is a simple implementation of an concreate PDF text reader command.
      The class can use `page range` and `regex` to extract specific text from the file. 
      The result is either file saved or text returned by `execute` method.

      Constructor(params)
          input     - must be a local file path.
          output    - local path (if not given, text file will be stored next to the input pdf).
          pages     - user given page range.
          tokens    - `regex` tokens (they must be fited for a JSON format).
          method    - 

      Methods:
          execute(self)       - evaluate given input params and extracts archive.
    """
    def __init__(self, log, params):
        super().__init__(log, params)

        assert isinstance(params['input'],  str) 
        assert isinstance(params['pages'],  list)
        assert isinstance(params['tokens'], list)
        assert isinstance(params['result'], str)

        self._input  = params['input']
        self._output = params['output'] if 'output' in params else WattleUtils.change_extension(self._input, ".txt")
        self._pages  = params['pages']
        self._tokens = params['tokens']
        self._result = params['result'] if 'result' in params else 'text'
        self._text   = ""

    def _read_text(self):
        self.log.debug("{}._read_text()".format(self.__class__.__name__))

        try:
            with open(self._input, 'rb') as f:
                reader = pypdf.PdfReader(f)
                page_range = range(len(reader.pages)) if len(self._pages) == 0 else self._pages

                for page_number in page_range:
                    page = reader.pages[page_number]
                    self._text += page.extract_text()
            
            self.log.info( f"Pages found: {len(reader.pages)}")
            self.log.info( f"Page range: {page_range};" )
            self.log.info( f"Text lenght: {len(self._text)}" )

        except Exception as e:
          msg = "Error reading pdf file: {}".format(self._input)
          self.log.error(msg)
          self.log.error(e)
          raise ReadingPdfFileError(msg)

    def _replace_tokens(self):
        self.log.debug("{}._replace_tokens()".format(self.__class__.__name__))
        if not len(self._tokens) > 0 : return 0
        for pattern in self._tokens:
            regex = re.compile(pattern)
            self._text = regex.sub(pattern, '', self._text)

    def _save_text(self):
        self.log.debug("{}._save_text()".format(self.__class__.__name__))
        if not self._output: 
            self.log.info(MSG_NOT_FOUND.format("Output file", self._output))
            return

        with open(self._output, "w+", encoding='utf-8') as f:
            f.write(self._text)

        self.log.info(MSG_FILE_SAVED.format(self._output))

    def extract(self):
        super().extract()

        if not os.path.exists(self._input):
            msg =  MSG_NOT_FOUND.format("File", self._input)
            self.log.error( msg )
            raise FileNotFoundError( msg )

        self._read_text()
        self._replace_tokens()
    
        if self._result == "save":
            self._save_text()
        elif self._result == "text":
            return self._text
        else:
            raise TypeError("Unknown method!")
