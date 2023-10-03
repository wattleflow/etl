import io
import os
import re
import sys
import pdfplumber
# import pypdf
import logging
import gc, psutil, traceback
import timeit, datetime
import pandas as pd
# import nltk
import spacy
import time
import warnings
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.multiprocessing
from etl.utils.lambda_functions import (
    inc,
    zero
)

from tqdm.notebook import tqdm
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from etl.core.concrete import WattleExtract
from etl.utils import WattleUtils
from etl.core.constants import (
    MSG_FILE_EXIST,
    MSG_FILE_OVERWRITEN,
    MSG_FILE_DOWNLOADED,
    METHOD_EXEC,
    MSG_NOT_FOUND,
    MSG_FILE_SAVED,
    EXTRACTOR_MIN_COUNT,
    EXTRACTOR_MAX_WORDS,
    EXTRACTOR_MAX_LENGTH,
    SUMMARISER_MAX_WORDS,
    TRANSLATOR_MAX_WORDS,
    TRANSLATOR_DEST,
    DEFAULT_MULTIPLIER,
    SUMMARY_TEXT,
)

class ReadingPdfFileError(Exception):
    pass

# PDF Paper Extractor
class PDFPapers(WattleExtract):
    """
      This class is a simple implementation of an concreate PDF Paper reader command.
      The class can use `page range` and `regex` to extract specific text from the file. 
      The result is saved as a CSV file with following format:
          { 'text', 'pg', 'paragraph', 'wc', 'chars' }

      Constructor(params)
          path      - must have a path given
          pages     - user given page range.
      Methods:
          execute(self)       - evaluate given input params and extracts archive.
    """
    def __init__(self, log, params):
        super().__init__(log, params)
        assert isinstance(params['path'], str)
        assert isinstance(params['min_words'], int)
        assert isinstance(params['max_words'], int)
        assert isinstance(params['max_length'], int)

        self._path = params['path']
        self._ext = '.pdf'
        self._pages  = params['pages'] if 'pages' in params else None
        self._min_count = params['min_count'] if 'min_count' in params else EXTRACTOR_MIN_COUNT
        self._max_words = params['max_words'] if 'max_words' in params else EXTRACTOR_MAX_WORDS
        self._max_length = params['max_length'] if 'max_length' in params else EXTRACTOR_MAX_LENGTH
        self._nlp = spacy.load("en_core_web_sm")
        self._lines = deque()
        self._progress = None
        self._filelist = deque() #         self._statlist = deque()
        self._paragraphs = deque()
        self._counter = 0
        self._text = ""
        self.extract()

    def _extract_paragraphs(self, text, pg):
        self.log.debug("{}._make_senteces()".format(self.__class__.__name__))
        self._counter += 1
        self._lines.clear()
        text = text.strip()
        text = re.sub(r'[^\x00-\x7F]+', '', text)
        text = re.sub(r'[\n]+', ' ', text)
        text = re.sub(r'[\s\s]+', ' ', text)
        wc = len(text.split())
        
        if self._min_count > wc:
            self.log.debug("{}._make_senteces(): {}".format(self.__class__.__name__, text))
            return

        pg, words, length = inc(pg),0,0 #         self.stats.add(pg, text)
        sentences = [ f"{s}" for s in self._nlp(text).sents ]
        for s in sentences:
            words += len(s.split())
            length += len(s)
            if words < self._max_words and length < self._max_length:
                self._lines.append(s.strip())
            else:
                text = ' '.join(self._lines); self._lines.clear();
                line = { 'text': text, 'pg': pg, 'paragraph': self._counter, 'wc': len(text.split()), 'chars': len(text) }
                self._paragraphs.append(line)
                self._lines.append(s)
                words += len(s.split())
                length += len(s)

        if len(self._lines) > 0:
            text = ' '.join(self._lines)
            line = { 'text': text, 'pg': pg, 'paragraph': self._counter, 'wc': len(text.split()), 'chars': len(text) }
            self._paragraphs.append(line)

    def _process_file(self, filename):
        self.log.debug("{}._process_file()".format(self.__class__.__name__))

        if not filename.endswith('.pdf'):
            msg = "File [{}] is not a PDF document.".format(filename)
            self.log.warning(msg)
            raise ReadingPdfFileError(msg)

        try:
            with pdfplumber.open(filename) as pdf:
                page_range = range(len(pdf.pages)) if not self._pages else self._pages
                self._progress = tqdm(total=len(page_range), desc=f"Extracting: [{filename}]")
                for page_num in page_range:
                    page = pdf.pages[page_num]
                    self._extract_paragraphs(page.extract_text(), page_num)
                    time.sleep(0.1)
                    self._progress.update(1)

            self.log.debug( f"Pages found: {len(pdf.pages)}")
            self.log.debug( f"Pages range: {page_range};" )

            df = pd.DataFrame(self._paragraphs)
            file_name = filename.replace(".pdf", ".csv")
            df.to_csv(file_name, index=False)
            del df
        except Exception as e:
            msg = f"{self.__class__}._process_file: {e}\n"
            msg += traceback.format_exc()
            raise Exception(msg)
        finally:
            gc.collect()

    def extract(self):
        super().extract()
        if not WattleUtils.path_exists(self._path):
            msg = MSG_NOT_FOUND.format("File", self._path)
            self.log.error(msg)
            raise FileNotFoundError(msg)

        for name in os.listdir(self._path):
            if name.endswith(self._ext):
                filename = os.path.join(self._path, name)
                self._filelist.append(filename)
#                 self._stats = Stats(name)
#                 self._statlist.append(self.stats)
                self._process_file(filename)
                self._counter += 1
#                 self.log.info(self.stats)

        if zero(self._filelist):
            msg=f"WARNING: Files {self._ext} not found."
            self.log.warning(msg)
            raise Exception(msg)