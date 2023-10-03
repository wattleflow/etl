import io
import os
import sys
import gc
import warnings
import pandas as pd
# import dask.dataframe as dd
import dask.multiprocessing # --> preporuceno za rad
from tqdm.notebook import tqdm
from collections import deque
from transformers import pipeline
from concurrent.futures import ThreadPoolExecutor

from etl.utils import WattleUtils
from etl.utils.lambda_functions import zero 
from etl.core.concrete import WattleTransform
from etl.core.constants import (
    SUMMARY_TEXT,
    MSG_NOT_FOUND,
    EMPTY
)

min_max_range = [
    (  0,    20, 0.85, 0.31),
    ( 20,    50, 0.75, 0.32),
    ( 50,   100, 0.74, 0.33),
    (100,   200, 0.73, 0.34),
    (200,   300, 0.72, 0.35),
    (300,   400, 0.71, 0.36),
    (400,   500, 0.70, 0.37)
]

def min_max(x):
    for n in min_max_range:
        if x > n[0] and x < n[1]:
            return int(x*n[2]), int(x*n[3])
    return int(x * 0.75), int(x * 0.35)

class SummariserError(Exception):
    pass

class HuggingFaceSummariser(WattleTransform):
    def __init__(self, log, params):
        super().__init__(log, params)
        assert isinstance(params['path'], str)
        assert isinstance(params['model'], str)
        assert isinstance(params['from_column'], str)
        assert isinstance(params['to_column'], str)
        assert isinstance(params['num_threads'], int)
        assert isinstance(params['min_threshold'], int)

        self._path          = params['path']
        self._ext           = ".csv"
        self._filelist      = deque()        #         self.statlist = deque()
        self._counter       = 0
        self._from_column   = params['from_column']
        self._to_column     = params['to_column']
        self._num_threads   = int(params['num_threads'])
        self._min_threshold = int(params['min_threshold'])
        self._drop          = params['drop'] if 'drop' in params else True
        self._summariser    = pipeline("summarization", model=params['model'])
        self._progress      = None
        self.transform()

    def transform(self):
        super().transform()
        if not WattleUtils.path_exists(self._path):
            msg = MSG_NOT_FOUND.format("Path", self._path)
            self.log.error(msg)
            raise FileNotFoundError(msg)

        for name in os.listdir(self._path):
            if name.endswith(self._ext):
                filename = os.path.join(self._path, name)
                self._filelist.append(filename) #                 self.stats = Stats(name)
                self.process_file(filename)     #                 self.statlist.append(self.stats)
                self._counter += 1              #                 self.log.info(self.stats)

        if zero(self._filelist):
            msg=f"WARNING: Files {self._ext} not found."
            self.log.warning(msg)
            raise SummariserError(msg)

    def _column_check(self, df):
        if not self._from_column in df.columns:
            raise Exception( f"Column {self._from_column} doesn't exist in csv file." )
        if self._to_column in df.columns:
            if self._drop == True: df.drop(self._to_column, axis=1, inplace=True)

    def _apply(self, text):
        self._counter += 1
        wc = len(f"{text}".split())
        if not wc >= self._min_threshold:        #             self.stats.add(self.counter, text);
            self._progress.update(1)
            return EMPTY
        max, min = min_max(wc)
        summary  = self._summariser(text, max_length=max, min_length=min, do_sample=False)[0][SUMMARY_TEXT] #         self.stats.add(self.counter, summary);
        self._progress.update(1)
        return summary if len(summary) > 0 else EMPTY

    def process_file(self, filename):
        df = None
        try:
            df = pd.read_csv(filename)
            self._column_check(df)
            self._progress = tqdm(total=len(df), desc=f"Summarisng: {filename}")
            with ThreadPoolExecutor(max_workers=self._num_threads) as executor:
                column = list(executor.map(self._apply, df[self._from_column]))
            df.insert(0, self._to_column, column)
            df.to_csv(filename, index=False)
        except Exception as e:
            msg = f"{self.__class__}._process_file: {e}\n"
            msg += traceback.format_exc()
            raise SummariserError(msg)
        finally:
            del df
            gc.collect()