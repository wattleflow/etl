import io
import os
import gc
import traceback
import pandas as pd

from googletrans import Translator
from tqdm.notebook import tqdm
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from etl.utils import WattleUtils
from etl.core.concrete import WattleTransform
from etl.utils.lambda_functions import (
    inc,
    zero
)
from etl.core.constants import (
    MSG_NOT_FOUND,
    MSG_FILE_SAVED,
    TRANSLATOR_MAX_WORDS,
    TRANSLATOR_DEST,
    EMPTY
)

class GoogleTranslationError(Exception):
    pass

class GoogleTranslator(WattleTransform):
    """
      This class is a simple implementation of Google translation API in a dataframe.
      An CSV files will be loaded into a DataFrame and given text column will be translated to a destination language. 
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
        assert isinstance(params['from_column'], str)
        assert isinstance(params['to_column'], str)
        assert isinstance(params['src'], str)
        assert isinstance(params['dest'], str)
        assert isinstance(params['min_threshold'], int)
        assert isinstance(params['num_threads'], int)
        self._path = params['path']
        self.ext = ".csv"
        self.from_column = params['from_column']
        self.to_column = params['to_column']
        self.src = params['src']
        self.dest = params['dest']
        self.drop = params['drop'] if 'drop' in params else True
        self.min_threshold = params['min_threshold']
        self.num_threads = params['num_threads']
        self.progress = None
        self.filelist = deque() #         self.statlist = deque()
        self.buffer = deque()
        self.counter = 0
        self.transform()

    def _apply(self, text):
        self.counter += 1;
        wc = len(f"{text}".split())
        if not wc >= self.min_threshold: #             self.stats.add(self.counter, '')
            self.progress.update(1)
            return EMPTY

        translator = Translator()
        translated = translator.translate(text, src=self.src, dest=self.dest).text #         self.stats.add(self.counter, translated)
        self.progress.update(1)
        return translated

    def _column_check(self, df):
        if not self.from_column in df.columns:
            raise GoogleTranslationError( f"Column {self.from_column} doesn't exist in csv file." )

        if self.to_column in df.columns:
            if self.drop == True: df.drop(self.to_column, axis=1, inplace=True)

    def process_file(self, filename):
        try:
            df = pd.read_csv(filename)
            self._column_check(df)
            self.progress = tqdm(total=len(df), desc=f"Translating: {filename}")
            num_threads = self.num_threads
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                column = list(executor.map(self._apply, df[self.from_column]))
            df.insert(0, self.to_column, column)
            df.to_csv(filename, index=False)
            del df
        except Exception as e:
            msg = f"{self.__class__}._process_file: {e}\n"
            # msg += traceback.format_exc()
            self.log.error(msg)
            raise GoogleTranslationError(msg)
        finally:
            gc.collect()

    def transform(self):
        super().transform()
        if not WattleUtils.path_exists(self._path):
            msg = MSG_NOT_FOUND.format("File", self._input)
            self.log.error(msg)
            raise FileNotFoundError(msg)

        for name in os.listdir(self._path):
            if name.endswith(self.ext):
                filename = os.path.join(self._path, name)
                self.filelist.append(filename) #                 self.stats = Stats(name)
                self.process_file(filename)    #                 self.statlist.append(self.stats)
                self.counter += 1              #                 self.log.info(self.stats)

        if zero(self.filelist):
            msg=f"WARNING: Files {self.ext} not found."
            self.log.warning(msg)
            raise GoogleTranslationError(msg)