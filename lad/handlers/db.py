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
            if self.database:
                conn_str += ';DATABASE={}'.format(self.database)
            if self.user and self.password:
                conn_str += ';UID={};PWD={}'.format(self.user, self.password)
            self._conn = pyodbc.connect(conn_str)
        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def sql_to_df(self, query, index_col=None):
        query = query.strip()
        query = query.replace("\n", " ")
        query = query.replace("\r", " ")
        if not self._conn:
            con = self.connect()
        else:
            con = self._conn
        out_df = pd.read_sql(query, con)
        if index_col:
            out_df.set_index(index_col, inplace=True)
        return out_df

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

    def delete_rows(self, table, schema, table_cols=None, delete_conds=None):
        # table_cols are the table columns you want to use to determine which
        #  rows to delete--must be list of strings
        # delete_conds are the values you want to use to determine which rows
        #  to delete--must be list of tuples, each tuple being same length as
        #  table_cols
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

        delete_sql_statement = 'DELETE FROM {0}.{1}'.format(schema, table)

        if table_cols:
            if not delete_conds:
                raise Exception('if table_cols is provided, delete_conds must also be provided')

            delete_cond_length_check = [
                len(tup)==len(table_cols) for tup in delete_conds
            ]
            if not all(delete_cond_length_check):
                raise Exception('every delete cond tuple is not the same '
                                'length as table_cols')

            delete_sql_statement += ' WHERE '

            num_or_conds = len(delete_conds)
            for or_idx, or_delete_cond in enumerate(delete_conds):
                num_and_conds = len(or_delete_cond)
                delete_sql_statement += '('
                for and_idx, and_cond in enumerate(or_delete_cond):
                    if and_cond is None:
                        delete_sql_statement += "{0} IS NULL".format(
                            table_cols[and_idx]
                        )
                    elif isinstance(and_cond, bool):
                        if and_cond:
                            table_val = 1
                        else:
                            table_val = 0
                        delete_sql_statement += "{0} = {1}".format(
                            table_cols[and_idx], table_val
                        )
                    elif isinstance(and_cond, str):
                        delete_sql_statement += "{0} = '{1}'".format(
                            table_cols[and_idx], and_cond
                        )
                    else:
                        delete_sql_statement += "{0} = {1}".format(
                            table_cols[and_idx], str(and_cond)
                        )
                    if and_idx + 1 < num_and_conds:
                        delete_sql_statement += ' AND '
                    else:
                        delete_sql_statement += ')'
                if or_idx+1 < num_or_conds:
                    delete_sql_statement += ' OR '

        curs = con.cursor()
        curs.execute(delete_sql_statement)
        curs.commit()
        curs.close()

    def delete_table(self, table, schema):
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
            # raising exception for this exercise
            # if I were building this functionality out I would try to infer
            # data types from the input df and create the table here
            raise Exception('table {0}.{1} does not exist'.format(
                schema, table
            ))

        delete_sql_statement = 'DROP TABLE {0}.{1}'.format(schema, table)

        curs = con.cursor()
        curs.execute(delete_sql_statement)
        curs.commit()
        curs.close()

    def create_table(self, table, schema, col_type_map):
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

        if table_exists:
            #raising exception for this exercise
            #if I were building this functionality out I would try to infer
            # data types from the input df and create the table here
            raise Exception('Table {0}.{1} already exists.  Please '
                            'delete existing table first'.format(
                schema, table
            ))

        create_sql_statement = 'CREATE TABLE {0}.{1} ('.format(schema, table)

        for col, col_type in col_type_map.items():
            create_sql_statement += '{0} {1},'.format(col, col_type)

        #remove last comma and add close parenthesis
        create_sql_statement = create_sql_statement[:-1]
        create_sql_statement += ')'

        curs = con.cursor()
        curs.execute(create_sql_statement)
        curs.commit()
        curs.close()

    def update_table(self, table, schema, update_df, join_col):
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
            # raising exception for this exercise
            # if I were building this functionality out I would try to infer
            # data types from the input df and create the table here
            raise Exception('table {0}.{1} does not exist'.format(
                schema, table
            ))

        base_update_sql_statement = 'UPDATE {0}.{1} SET '.format(schema, table)
        curs = con.cursor()
        for idx, row in update_df.iterrows():
            update_sql_statement = base_update_sql_statement
            cols_to_update = row.index.tolist()
            cols_to_update = [i for i in cols_to_update if i != join_col]
            for col in cols_to_update:
                if row[col] is None:
                    update_sql_statement += '{0} = NULL,'.format(col, row[col])
                elif isinstance(row[col], bool):
                    if row[col]:
                        table_val = 1
                    else:
                        table_val = 0
                    update_sql_statement += '{0} = {1},'.format(col, table_val)
                elif isinstance(row[col], str):
                    update_val = row[col].replace("'","''")
                    update_sql_statement += "{0} = '{1}',".format(col, update_val)
                else:
                    update_sql_statement += '{0} = {1},'.format(col, row[col])

            # remove last comma
            update_sql_statement = update_sql_statement[:-1]

            if row[join_col] is None:
                update_sql_statement += ' WHERE {0} = NULL'.format(join_col)
            elif isinstance(row[join_col], bool):
                if row[col]:
                    table_val = 1
                else:
                    table_val = 0
                    update_sql_statement += ' WHERE {0} = {1}'.format(join_col, table_val)
            elif isinstance(row[join_col], str):
                update_sql_statement += " WHERE {0} = '{1}'".format(join_col, row[join_col])
            else:
                update_sql_statement += ' WHERE {0} = {1}'.format(join_col, row[join_col])

            #print(update_sql_statement)
            curs.execute(update_sql_statement)
            curs.commit()

        curs.close()