import os
import pandas as pd

from etl.core.constants import MSG_NOT_FOUND
from etl.core.concrete import WattleExtract
from etl.utils.base import WattleUtils

fieldtype = {
    'int'    : 'INTEGER',
    'bool'   : 'BOOLEAN',
    'int64'  : 'BIGINT',
    'float'  : 'DECIMAL',
    'float64': 'DOUBLE PRECISION',
    'object' : 'VARCHAR(255)'
}

class AutoPostgreSql(WattleExtract):
    def __init__(self, log, params):
        super().__init__(log, params)
        assert isinstance(params['config'],         dict)
        assert isinstance(params['staging-tables'], dict)
        assert isinstance(params['star-tables'],    dict)
        assert isinstance(params['star-inserts'],   dict)
        
        self.config = params['config']

        self.staging_tables = params['staging-tables']
        self.star_tables = params['star-tables']
        self.star_inserts = params['star-inserts']

        self._sql_staging_del = []
        self._sql_staging_cr  = []
        self._sql_staging_cp  = []

        self._sql_star_del    = []
        self._sql_star_cr     = []
        self._sql_star_ins    = []

    def _next(self):
        self._counter += 1
        return self._counter

    def _staging_drop(self, name):
        self.log.info(f'stage-drop: {name}')

        sql = f"DROP TABLE IF EXISTS staging_{name};\n"
        self._sql_staging_del.append(sql)
        self.log.debug(sql)

    def _staging_create(self, name):
        self.log.info(f'stage-create: {name}')
        file_path = self.staging_tables[name]['input']
        df = pd.read_csv(file_path)

        sql = f"CREATE TABLE IF NOT EXISTS staging_{name} (\n"
        for field, dtype in df.dtypes.items():
            field_type = fieldtype[str(dtype)] if str(dtype) in fieldtype else "UNKNOWN"
            sql += f"     \"{field}\" {field_type},\n"
        sql = sql[:-2] + "\n"
        sql += ");\n\n"

        self._sql_staging_cr.append(sql)
        self.log.debug(sql)

    def _staging_copy(self, name):
        self.log.info(f'stage-copy: {name}')
        file_path = self.staging_tables[name]['input']

        sql = f"COPY staging_{name} FROM '{file_path}'\n"
        sql += "WITH (FORMAT CSV, HEADER True);\n\n"

        self._sql_staging_cp.append(sql)
        self.log.debug(sql)

    def _star_drop(self, name):
        self.log.info(f'star-drop: {name}')
        sql = f"DROP TABLE IF EXISTS {name};\n"
        self._sql_star_del.append(sql)
        self.log.debug(sql)

    def _star_create(self, name):
        self.log.info(f'star-create: {name}')
        sql = self.params['star-tables'][name]
        self._sql_star_cr.append(sql)
        self.log.debug(sql)

    def _star_inserts(self):
        self.log.info(f'star-inserts')
        for sql in self.star_inserts.values():
            self._sql_star_ins.append(sql)

    def _save_to_file(self, file_path, statments):
        with open(file_path, "w") as file:
            for sql in statments:
                file.write(sql)

    def _generate_scripts(self):
        for table in self.staging_tables:
            self._staging_drop(table)
            self._staging_create(table)
            self._staging_copy(table)

        for table in self.star_tables:
            self._star_drop(table)
            self._star_create(table)

        self._star_inserts()

    def _save_scripts(self):
        sql_path = self.config['sql_path']
        if not os.path.exists(sql_path):
            raise FileNotFoundError("File not found: {}".format(sql_path))

        self._save_to_file(f'{sql_path}1-staging-drop.sql', self._sql_staging_del)
        self._save_to_file(f'{sql_path}2-staging-create.sql', self._sql_staging_cr)
        self._save_to_file(f'{sql_path}3-staging-copy.sql', self._sql_staging_cp)

        self._save_to_file(f'{sql_path}4-star-drop.sql', self._sql_star_del)
        self._save_to_file(f'{sql_path}5-star-create.sql', self._sql_star_cr)
        self._save_to_file(f'{sql_path}6-star-inserts.sql', self._sql_star_ins)

    def extract(self):
        super().extract()
        if not WattleUtils.path_exists(self.config['data_sources']):
            msg = MSG_NOT_FOUND.format("Data Sources: {}".format(self.config['data_sources']))
            self.log.error(msg)
            raise FileNotFoundError(msg)

        if not WattleUtils.path_exists(self.config['data_path']):
            msg = MSG_NOT_FOUND.format("Data Path: {}".format(self.config['data_path']))
            self.log.error(msg)
            raise FileNotFoundError(msg)
        self._generate_scripts()
        self._save_scripts()
            
## TIMEFORMAT 'epochmillisecs', BLANKSASNULL true, EMPTYASNULL true TRUNCATECOLUMNS