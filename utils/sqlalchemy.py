import re
import os
import time
import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

class WattleSqlAlchemyError(Exception):
    pass

class WattleSqlAlchemy:
    def __init__(self, **kwargs):
        _user = kwargs['user'] if 'user' in kwargs else None
        _pswd = kwargs['pswd'] if 'pswd' in kwargs else None
        _host = kwargs['host'] if 'host' in kwargs else None
        _port = kwargs['port'] if 'port' in kwargs else None
        _db   = kwargs['dbname'] if 'dbname' in kwargs else None
        self.logger  = kwargs['logger'] if 'logger' in kwargs else None
        self.verbose = kwargs['verbose'] if 'verbose' in kwargs else False

        self.config = f"postgresql+psycopg2://{_user}:{_pswd}@{_host}:{_port}/{_db}"
        self.engine = create_engine(self.config)
        self.conn = self.engine.connect()
        connstr = re.sub(r":\w+@", ":***@", self.config)
        if self.verbose: self.log(f"Connected: {connstr}")

    def log(self, text):
        if self.logger:
            self.logger.info(text)
        elif self.verbose:
            current_time = datetime.datetime.now()
            milliseconds = current_time.microsecond // 1000 
            formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
            entry = f"{formatted_time}.{milliseconds}: {text}"
            print(entry)

    def query(self, sql):
        try:
            return pd.read_sql_query(sql, self.conn)
        except Exception as e:
            error = {
                'name' : '{}'.format(type(self).__name__), 
                'error': '{}'.format(e),
                'sql'  : f'{sql}'
            }
            return pd.DataFrame(error, index=[0])

    def close(self):
        self.conn.close()

    def count(self, tablename, details=False):
        df1 = self.query(f"SELECT COUNT(*) FROM {tablename};", details)
        return df.iloc[0,0]