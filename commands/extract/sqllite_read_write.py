import sqlite3
import pandas as pd
from pandas.errors import IntCastingNaNError

from etl.core.constants import (
    MSG_FILE_EXIST,
    METHOD_EXEC,
    MSG_CSV_FILE_ERROR,
    MSG_FILE_CREATED
)


from etl.core.concrete import WattleExtract
from etl.core.utils import WattleUtils

class SQLiteReadError(Exception):
    pass

class SQLiteWriteError(Exception):
    pass

class SQLiteReadWrite(WattleExtract):
    """
      This class is a simple implementation of an SQL Lite read/write command.
      With a given `params` class uses pandas DataFrame to read data from a given
      CSV and save the tables from database to the CSV. This class is written to 
      demonstrate opearation with database in support of writing a code for 
      enterprise relationioanl databases.

      Constructor(params)
          input     - must be a local file path.
          output    - local path (if not given, text file will be stored next to 
                      the input pdf).
          columns   - csv data fields.
          renamed   - new column names.
          unique    - produce unique values for a given columns (list).
          min-maxm  - create log entries for min/max values of a given columns (list).
          overwrite - True|False.

      Methods:
          execute(self) - evaluate given input params and extracts archive.
    """

    def __init__(self, log, params):
        super().__init__(log, params)
        assert isinstance(params, dict)
        assert isinstance(params['connection'], str)

        self._params = params
        self._connection = params['connection']
        self._input = []
        self._output = []

    def _connect(self):
        self.log.debug("{}._connect()".format(self.__class__.__name__))
        self.conn = sqlite3.connect(self._connection)

    def _convert_fields(self, df, columns):
        self.log.debug("{}._convert_fields()".format(self.__class__.__name__))

        try:
            for field, data_type in columns.items():
                df[field] = df[field].astype(data_type)
        except IntCastingNaNError as e:
            self.log.error("NaN casting error: {}".format(e))
        except Exception as e:
            self.log.error("Unknown error: {}".format(e))

        return df

    def _read_from_csv(self):
        self.log.debug("{}._read_from_csv()".format(self.__class__.__name__))
        try:
            file_path = None
            for name, table in self._params['tables'].items():
                file_path = table['input'] if 'input' in table else None
                columns = table['columns'] if 'columns' in table else None
                dropna  = table['dropna'] if 'dropna' in table else None

                self.log.info("table name: {}".format(name))
                self.log.info("file path: {}".format(file_path))

                if not file_path is None:
                    if not columns is None:
                        self.log.info("columns: {}".format(list(columns)))
                        df = pd.read_csv(file_path, usecols=list(columns))
                        if dropna: df.dropna(subset=dropna, inplace=True)
                        df = self._convert_fields(df, columns)
                    else:
                        df = pd.read_csv(file_path)
                    
                    df.to_sql(name, self.conn, if_exists='replace', index=False)
                    self.log.info("sqlite: stored: {}".format(name))
        except Exception as e:
            msg = MSG_CSV_FILE_ERROR.format(file_path)
            self.log.error(msg)
            self.log.error(e)
            raise SQLiteReadError(msg)

    def _write_to_csv(self):
        self.log.debug("{}._write_to_csv()".format(self.__class__.__name__))
        try:
            file_path = None
            for name, table in self._params['tables'].items():
                file_path = table['output'] if 'output' in table else None
                columns = table['columns'] if 'columns' in table else None

                self.log.info("table name: {}".format(name))
                self.log.info("file path: {}".format(file_path))

                if not file_path is None:
                    if not columns is None:
                        self.log.info("columns: {}".format(list(columns)))
                        df = pd.read_sql_query("SELECT {} FROM {}".format(",".join(columns), name), self.conn)
                        df = self._convert_fields(df, columns)
                        df.to_csv(file_path, header=True, index=False)
                    else:
                        df = pd.read_sql_query("SELECT * FROM {}".format(name), self.conn)
                        df.to_csv(file_path, header=True, index=False)  

                    self.log.info("sqlite: saved to file: {}: {}".format(name, file_path))
        except Exception as e:
            msg = MSG_CSV_FILE_ERROR.format(file_path)
            self.log.error(msg)
            self.log.error(e)
            raise SQLiteReadError(msg) 

    def extract(self):
        super().extract()

        self._connect()
        self._read_from_csv()
        self._write_to_csv()
        return self.conn
