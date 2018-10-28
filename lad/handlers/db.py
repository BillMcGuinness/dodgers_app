import pyodbc
import pandas as pd

from lad.handlers.static import db_configs

class DbHandler(object):

    def __init__(self, config_key):
        if not hasattr(db_configs, config_key):
            raise Exception('[{}] db config does not exist'.format(config_key))
        _config = getattr(db_configs, config_key)
        self.server = _config.get('server')
        self.driver = _config.get('driver')
        self.database = _config.get('database')
        self.user = _config.get('user')
        self.password = _config.get('password')
        self.odbc_module = _config.get('odbc_module')

        self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.disconnect()

    def connect(self):
        if self._conn is None:
            conn_str = 'DRIVER={%s};SERVER=%s' % (self.driver, self.server)
            conn_msg = conn_str
            if self.database:
                conn_str += ';DATABASE={}'.format(self.database)
                conn_msg += ';DATABASE={}'.format(self.database)
            if self.user and self.password:
                conn_str += ';UID={};PWD={}'.format(self.user, self.password)
                conn_msg += ';UID={};PWD=****'.format(self.user)
            self._conn = pyodbc.connect(conn_str)
        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def sql_to_df(self, query):
        query = query.strip()
        query = query.replace("\n", " ")
        query = query.replace("\r", " ")
        if not self._conn:
            con = self.connect()
        else:
            con = self._conn
        return pd.read_sql(query, con)

    def df_to_table(self, df, table, schema, append=True):
        con = self.connect()

        if self.driver == 'SQL Server':
            # different ways of determining if a table already exists for
            # different drivers
            table_exists_df = self.sql_to_df(
                query="""
                    SELECT *
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{0}'
                        AND TABLE_NAME = '{1}'
                """.format(schema, table)
            )
            if not table_exists_df.empty:
                table_exists = True
            else:
                table_exists = False

        if not table_exists:
            #raising exception for this exercise
            #if I were building this functionality out I would try to infer
            # data types from the input df and create the table here
            raise Exception('table {0}.{1} does not exist'.format(
                schema, table
            ))

        curs = con.cursor()

        if not append:
            curs.execute(
                """
                DELETE FROM {0}.{1}
                """.format(schema, table)
            )

        df_cols = ['[{}]'.format(c) for c in list(df.columns)]
        df_cols_str = ','.join(df_cols)
        placeholders = ','.join(['?'] * len(df_cols))
        for index, row in df.iterrows():
            curs.execute(
                """
                INSERT INTO {0}.{1}({2})
                VALUES ({3})
                """.format(
                    schema, table, df_cols_str, placeholders
                ),
                tuple(row)
            )
            curs.commit()

        curs.close()
