import os
import re
import time
import datetime
import psycopg2
import numpy as np
import pandas as pd

from etl.core.constants import MSG_NOT_FOUND

class WattlePostgresError(Exception):
    pass

class WattlePostgres:
    def __init__(self, **kwargs):
        _user = kwargs['user'] if 'user' in kwargs else None
        _pswd = kwargs['pswd'] if 'pswd' in kwargs else None
        _host = kwargs['host'] if 'host' in kwargs else None
        _port = kwargs['port'] if 'port' in kwargs else None
        _dbname = kwargs['dbname'] if 'dbname' in kwargs else None
        self.logger  = kwargs['logger'] if 'logger' in kwargs else None
        self.verbose = kwargs['verbose'] if 'verbose' in kwargs else True
        self.conn=psycopg2.connect(
            host=_host,
            port=_port,
            user=_user,
            password=_pswd,
            database=_dbname
        )
        self.cursor = self.conn.cursor()

    def log(self, text):
        if self.logger:
            self.logger.info(text)
        elif self.verbose:
            current_time = datetime.datetime.now()
            milliseconds = current_time.microsecond // 1000 
            formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
            entry = f"{formatted_time}.{milliseconds}: {text}"
            print(entry)

    def read_sql_file(self, file_path):       
        if not os.path.exists(self._input):
            msg = MSG_NOT_FOUND.format("File", self._input)
            self.log(msg)
            raise FileNotFoundError(msg)

        with open(file_path, 'r') as file:
            return file.read()

    def execute(self, sql):
        try:
            self.cursor.execute(sql)
            return self.cursor.rowcount
        except Exception as e:
            msg = f"ERROR: WattlePostgress.execute: {e}"
            raise Exception(f"{msg}\n{sql}")

    def executemany(self, sql, data):
        self.cursor.executemany(sql, data)

    def selects(self, lista):
        assert isinstance(lista, list)
        res = []
        for sql in lista:
            self.cursor.execute(sql)
            res.append( self.cursor.fetchall() )
        return res

    def copy(self, file_path, table):
        if not os.path.exists(self._input):
            msg = MSG_NOT_FOUND.format("File", self._input)
            self.log(msg)
            raise FileNotFoundError(msg)
        with open(file_path, 'r') as file:
            self.cursor.copy_from(file.read(), table) 
        self.commit()

    def expert(self, sql, file):
        self.cursor.copy_expert(file, path)
        self.commit()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()
