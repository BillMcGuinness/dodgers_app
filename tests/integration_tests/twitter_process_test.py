import unittest
import lad
import pandas as pd
from jobs.twitter.dodgers_twitter import process_df

class twitterProcessTest(unittest.TestCase):

    def _setup_table_with_data(self, table_nm, schema, create_table_col_map, df):

        with lad.DbHandler('LA_DB') as db:
            db.create_table(table_nm, schema, create_table_col_map)
            db.df_to_table(df, table_nm, schema, append=False)

    def _setup_table_without_data(self, table_nm, schema, create_table_col_map):
        with lad.DbHandler('LA_DB') as db:
            db.create_table(table_nm, schema, create_table_col_map)

    def _teardown_table(self, table_nm, schema):
        with lad.DbHandler('LA_DB') as db:
            db.delete_table(table_nm, schema)

    def test_process_df_update(self):
        test_table = 'zzz_test'
        test_schema = 'dbo'
        create_table_col_map = {
            'id': 'NVARCHAR(100)',
            'b': 'NVARCHAR(100)',
            'c': 'BIT',
            'd': 'INT',
            'etl_created_date': 'NVARCHAR(100)',
            'etl_created_by': 'NVARCHAR(100)',
        }
        df = pd.DataFrame(data={
            'id': ['a', 'b', 'c', 'd', 'e'],
            'b': ['w', 'x', 'y', 'z', 'p'],
            'c': [True, True, False, False, True],
            'd': [10, 11, 12, 13, 14],
            'etl_created_date': ['123','345','456','67','6756'],
            'etl_created_by': ['123','345','456','67','6756']
        })
        self._setup_table_with_data(
            test_table, test_schema, create_table_col_map, df
        )

        new_data = pd.DataFrame(data={
            'id': ['a', 'x', 'y', 'z'],
            'b': ['different w', 'x', 'y', 'z',],
            'c': [True, True, False, False,],
            'd': [10, 11, 12, 13,],
            'etl_created_date': ['123', '345', '456', '67',],
            'etl_created_by': ['123', '345', '456', '67',]
        })
        new_data.set_index('id', inplace=True)

        process_df(
            new_data, test_schema + '.' + test_table, update_if_diff=True
        )

        with lad.DbHandler('LA_DB') as db:
            resulting_df = db.sql_to_df(
                query="""
                    SELECT *
                    FROM {0}.{1}
                """.format(test_schema, test_table)
            )

        self.assertEqual(len(resulting_df), 8)
        self.assertEqual(len(resulting_df.columns), 6)
        self.assertEqual(
            resulting_df.loc[
                resulting_df['id']=='a', 'b'
            ][0],
            'different w'
        )
        self._teardown_table(test_table, test_schema)

    def test_process_df_diff_ignore_cols(self):
        test_table = 'zzz_test'
        test_schema = 'dbo'
        create_table_col_map = {
            'id': 'NVARCHAR(100)',
            'b': 'NVARCHAR(100)',
            'c': 'BIT',
            'd': 'INT',
            'etl_created_date': 'NVARCHAR(100)',
            'etl_created_by': 'NVARCHAR(100)',
        }
        df = pd.DataFrame(data={
            'id': ['a', 'b', 'c', 'd', 'e'],
            'b': ['w', 'x', 'y', 'z', 'p'],
            'c': [True, True, False, False, True],
            'd': [10, 11, 12, 13, 14],
            'etl_created_date': ['123', '345', '456', '67', '6756'],
            'etl_created_by': ['123', '345', '456', '67', '6756']
        })
        self._setup_table_with_data(
            test_table, test_schema, create_table_col_map, df
        )

        new_data = pd.DataFrame(data={
            'id': ['a', 'x', 'y', 'z'],
            'b': ['different w', 'x', 'y', 'z', ],
            'c': [True, True, False, False, ],
            'd': [10, 11, 12, 13, ],
            'etl_created_date': ['different 123', '345', '456', '67', ],
            'etl_created_by': ['different 123', '345', '456', '67', ]
        })
        new_data.set_index('id', inplace=True)

        process_df(
            new_data, test_schema + '.' + test_table, update_if_diff=True
        )

        with lad.DbHandler('LA_DB') as db:
            resulting_df = db.sql_to_df(
                query="""
                            SELECT *
                            FROM {0}.{1}
                        """.format(test_schema, test_table)
            )

        self.assertEqual(len(resulting_df), 8)
        self.assertEqual(len(resulting_df.columns), 6)
        self.assertEqual(
            resulting_df.loc[
                resulting_df['id'] == 'a', 'b'
            ][0],
            'different w'
        )
        self.assertEqual(
            resulting_df.loc[
                resulting_df['id'] == 'a', 'etl_created_date'
            ][0],
            '123'
        )
        self.assertEqual(
            resulting_df.loc[
                resulting_df['id'] == 'a', 'etl_created_by'
            ][0],
            '123'
        )
        self._teardown_table(test_table, test_schema)

    def test_process_df_audit(self):
        test_table = 'zzz_test'
        test_audit_table = 'zzz_test_audit'
        test_schema = 'dbo'
        create_table_col_map = {
            'id': 'NVARCHAR(100)',
            'b': 'NVARCHAR(100)',
            'c': 'BIT',
            'd': 'INT',
            'etl_created_date': 'NVARCHAR(100)',
            'etl_created_by': 'NVARCHAR(100)',
        }
        df = pd.DataFrame(data={
            'id': ['a', 'b', 'c', 'd', 'e'],
            'b': ['w', 'x', 'y', 'z', 'p'],
            'c': [True, True, False, False, True],
            'd': [10, 11, 12, 13, 14],
            'etl_created_date': ['123', '345', '456', '67', '6756'],
            'etl_created_by': ['123', '345', '456', '67', '6756']
        })
        self._setup_table_with_data(
            test_table, test_schema, create_table_col_map, df
        )
        self._setup_table_without_data(
            test_audit_table, test_schema, create_table_col_map
        )

        new_data = pd.DataFrame(data={
            'id': ['a', 'x', 'y', 'z', 'b'],
            'b': ['different w', 'x', 'y', 'z', 'x'],
            'c': [True, True, False, False, True],
            'd': [10, 11, 12, 13, 11],
            'etl_created_date': ['different 123', '345', '456', '67', '345'],
            'etl_created_by': ['different 123', '345', '456', '67', '345']
        })
        new_data.set_index('id', inplace=True)

        process_df(
            new_data.copy(), test_schema + '.' + test_table, update_if_diff=True,
            audit_table=test_schema + '.' + test_audit_table
        )

        with lad.DbHandler('LA_DB') as db:
            resulting_audit_df = db.sql_to_df(
                query="""
                    SELECT *
                    FROM {0}.{1}
                """.format(test_schema, test_audit_table)
            )

        #all new and different rows should get inserted to audit table
        # id 'b' is exactly the same so shouldn't get inserted
        expected_audit_df = new_data.reset_index()
        #expected_audit_df.drop()
        expected_audit_df = expected_audit_df[expected_audit_df['id']!='b']
        expected_audit_df = expected_audit_df.sort_values('id')
        expected_audit_df = expected_audit_df.reindex_axis(
            sorted(expected_audit_df.columns), axis=1
        )
        expected_audit_df = expected_audit_df.reset_index()
        expected_audit_df = expected_audit_df.drop(
            'index', axis=1
        )


        resulting_audit_df = resulting_audit_df.sort_values('id')
        resulting_audit_df = resulting_audit_df.reindex_axis(
            sorted(resulting_audit_df.columns), axis=1
        )
        resulting_audit_df = resulting_audit_df.reset_index()
        resulting_audit_df = resulting_audit_df.drop(
            'index', axis=1
        )


        pd.testing.assert_frame_equal(expected_audit_df,resulting_audit_df)
        self._teardown_table(test_table, test_schema)
        self._teardown_table(test_audit_table, test_schema)

if __name__ == '__main__':
    unittest.main()