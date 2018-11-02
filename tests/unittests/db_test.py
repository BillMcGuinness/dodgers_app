import unittest
import lad
import pandas as pd

class dbTest(unittest.TestCase):

    def _setup_table_with_data(self, table_nm, schema, create_table_col_map, df):

        with lad.DbHandler('LA_DB') as db:
            db.create_table(table_nm, schema, create_table_col_map)
            db.df_to_table(df, table_nm, schema, append=False)

    def _teardown_table(self, table_nm, schema):
        with lad.DbHandler('LA_DB') as db:
            db.delete_table(table_nm, schema)

    def test_create_read_delete_table(self):
         create_table_col_map = {
             'a': 'NVARCHAR(100)',
             'b': 'DATETIME',
             'c': 'BIT',
             'd': 'INT'
         }

         table_nm = 'zzz_test'
         schema = 'dbo'

         with lad.DbHandler('LA_DB') as db:
             db.create_table(table_nm, schema, create_table_col_map)

             information_schema_df_1 = db.sql_to_df(
                 query="""
                    SELECT *
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_SCHEMA = '{0}'
                        AND c.TABLE_NAME = '{1}'
                 """.format(schema, table_nm)
             )

             self.assertEqual(
                 len(create_table_col_map), len(information_schema_df_1)
             )

             db.delete_table(table_nm, schema)

             information_schema_df_2 = db.sql_to_df(
                 query="""
                     SELECT *
                     FROM INFORMATION_SCHEMA.COLUMNS c
                     WHERE c.TABLE_SCHEMA = '{0}'
                         AND c.TABLE_NAME = '{1}'
                  """.format(schema, table_nm)
             )

             self.assertEqual(0, len(information_schema_df_2))

    def test_sql_to_df_with_index(self):
        test_table = 'zzz_test'
        test_schema = 'dbo'
        create_table_col_map = {
            'a': 'NVARCHAR(100)',
            'b': 'NVARCHAR(100)',
            'c': 'BIT',
            'd': 'INT'
        }
        df = pd.DataFrame(data={
            'a': ['a', 'b', 'c', 'd'],
            'b': ['w', 'x', 'y', 'z'],
            'c': [True, True, False, False],
            'd': [10, 11, 12, 13]
        })
        self._setup_table_with_data(
            test_table,test_schema, create_table_col_map, df
        )
        with lad.DbHandler('LA_DB') as db:
            df = db.sql_to_df(
                query="""
                    SELECT *
                    FROM {0}.{1}
                """.format(test_schema, test_table),
                index_col='a'
            )

        self.assertEqual(df.index.name, 'a')

        self._teardown_table(test_table, test_schema)

    def test_df_to_db_append(self):
        test_table = 'zzz_test'
        test_schema = 'dbo'
        create_table_col_map = {
            'a': 'NVARCHAR(100)',
            'b': 'NVARCHAR(100)',
            'c': 'BIT',
            'd': 'INT'
        }
        df = pd.DataFrame(data={
            'a': ['a', 'b', 'c', 'd'],
            'b': ['w', 'x', 'y', 'z'],
            'c': [True, True, False, False],
            'd': [10, 11, 12, 13]
        })
        self._setup_table_with_data(
            test_table, test_schema, create_table_col_map, df
        )

        new_data_df = pd.DataFrame(data={
            'a': ['zzz','xxx','yyy']
        })

        with lad.DbHandler('LA_DB') as db:
            current_table_size = db.sql_to_df(
                query="""
                    SELECT COUNT(*)
                    FROM {0}.{1}
                """.format(test_schema, test_table)
            ).iloc[0,0]
            db.df_to_table(new_data_df,test_table,test_schema, append=True)
            new_table_size = db.sql_to_df(
                query="""
                    SELECT COUNT(*)
                    FROM {0}.{1}
                """.format(test_schema, test_table)
            ).iloc[0, 0]

            self.assertEqual(
                len(new_data_df) + current_table_size,new_table_size
            )

        self._teardown_table(test_table, test_schema)

    def test_df_to_db_no_append(self):
        test_table = 'zzz_test'
        test_schema = 'dbo'
        create_table_col_map = {
            'a': 'NVARCHAR(100)',
            'b': 'NVARCHAR(100)',
            'c': 'BIT',
            'd': 'INT'
        }
        df = pd.DataFrame(data={
            'a': ['a', 'b', 'c', 'd'],
            'b': ['w', 'x', 'y', 'z'],
            'c': [True, True, False, False],
            'd': [10, 11, 12, 13]
        })
        self._setup_table_with_data(
            test_table, test_schema, create_table_col_map, df
        )

        new_data_df = pd.DataFrame(data={
            'a': ['zzz','xxx','yyy']
        })

        with lad.DbHandler('LA_DB') as db:
            db.df_to_table(new_data_df,test_table,test_schema, append=False)
            new_table_size = db.sql_to_df(
                query="""
                    SELECT COUNT(*)
                    FROM {0}.{1}
                """.format(test_schema, test_table)
            ).iloc[0, 0]

            self.assertEqual(
                len(new_data_df) ,new_table_size
            )

        self._teardown_table(test_table, test_schema)

    def test_delete_some_rows(self):
        test_table = 'zzz_test'
        test_schema = 'dbo'
        create_table_col_map = {
            'a': 'NVARCHAR(100)',
            'b': 'NVARCHAR(100)',
            'c': 'BIT',
            'd': 'INT'
        }
        df = pd.DataFrame(data={
            'a': ['a', 'b', 'c', 'd', None],
            'b': ['w', 'x', 'y', 'z', 'p'],
            'c': [True, True, False, False, True],
            'd': [10, 11, 12, 13, 14]
        })
        self._setup_table_with_data(
            test_table, test_schema, create_table_col_map, df
        )
        delete_cols = ['a','b','c','d']
        delete_vals = [(None,'p',True,14),('a','b',False,15)]
        with lad.DbHandler('LA_DB') as db:
            db.delete_rows(test_table, test_schema, delete_cols, delete_vals)
            row_count = db.sql_to_df(
                query="""
                    SELECT COUNT(*)
                    FROM {0}.{1}
                """.format(test_schema, test_table)
            ).iloc[0,0]

        self.assertEqual(4, row_count)
        self._teardown_table(test_table, test_schema)

    def test_delete_all_rows(self):
        test_table = 'zzz_test'
        test_schema = 'dbo'
        create_table_col_map = {
            'a': 'NVARCHAR(100)',
            'b': 'NVARCHAR(100)',
            'c': 'BIT',
            'd': 'INT'
        }
        df = pd.DataFrame(data={
            'a': ['a', 'b', 'c', 'd', None],
            'b': ['w', 'x', 'y', 'z', 'p'],
            'c': [True, True, False, False, True],
            'd': [10, 11, 12, 13, 14]
        })
        self._setup_table_with_data(
            test_table, test_schema, create_table_col_map, df
        )
        with lad.DbHandler('LA_DB') as db:
            db.delete_rows(test_table, test_schema)
            row_count = db.sql_to_df(
                query="""
                    SELECT COUNT(*)
                    FROM {0}.{1}
                """.format(test_schema, test_table)
            ).iloc[0,0]

        self.assertEqual(0, row_count)
        self._teardown_table(test_table, test_schema)

if __name__ == '__main__':
    unittest.main()