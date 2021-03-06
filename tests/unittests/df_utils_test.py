import unittest
import pandas as pd
from lad.utils import df_utils


class dfUtilsTest(unittest.TestCase):

    def test_diff_rows_no_diff(self):
        inp_df = pd.DataFrame(data={
            'idx': ['a','b','c','d','e','f'],
            'col1': [1,2,3,4,5,6],
            'col2': ['abc','def','xyz','hij','lmn','qrs']
        })
        inp_df.set_index('idx', inplace=True)

        inp_comp_df = inp_df.copy()

        exp_df = inp_df.assign(**{
            'is_diff': [False, False, False, False, False, False, ]
        })

        got_df = df_utils.diff_rows(inp_df, inp_comp_df, out_col='is_diff')
        pd.testing.assert_frame_equal(exp_df, got_df)

    def test_diff_rows_yes_diff(self):
        inp_df = pd.DataFrame(data={
            'idx': ['a','b','c','d','e','f','g'],
            'col1': [1,2,3,4,5,6,7],
            'col2': ['abc','def','xyz','hij','lmn','qrs', 'tyt']
        })
        inp_df.set_index('idx', inplace=True)

        inp_comp_df = inp_df.assign(**{
            'col2': ['abc','def','xyz','hij','lmn','xxx', None]
        })

        exp_df = inp_df.assign(**{
            'is_diff': [False, False, False, False, False, True, True]
        })

        got_df = df_utils.diff_rows(inp_df, inp_comp_df, out_col='is_diff')
        pd.testing.assert_frame_equal(exp_df, got_df)

    def test_diff_rows_extra_idxs(self):
        # inp df index 'g' doesn't exist in comp df, so it shouldn't be in
        # output df
        inp_df = pd.DataFrame(data={
            'idx': ['a','b','c','d','e','f','g'],
            'col1': [1,2,3,4,5,6, 7],
            'col2': ['abc','def','xyz','hij','lmn','qrs', 'tuv']
        })
        inp_df.set_index('idx', inplace=True)

        inp_comp_df = inp_df.assign(**{
            'col2': ['abc','def','xyz','hij','lmn','xxx', None]
        })
        inp_comp_df = inp_comp_df[~inp_comp_df.index.isin(['g'])]

        exp_df = inp_df.assign(**{
            'is_diff': [False, False, False, False, False, True, None]
        })
        exp_df = exp_df[~exp_df.index.isin(['g'])]

        got_df = df_utils.diff_rows(inp_df, inp_comp_df, out_col='is_diff')
        pd.testing.assert_frame_equal(exp_df, got_df)

    def test_diff_rows_exclude_cols(self):
        inp_df = pd.DataFrame(data={
            'idx': ['a','b','c','d','e','f','g'],
            'col1': [1,2,3,4,5,6,7],
            'col2': ['abc','def','xyz','hij','lmn','qrs', 'tuv'],
            'col2_xxx': ['abc','def','xyz','hij','lmn','qrs', 'tuv'],
            'newcol_pat': ['aaa','bbb','ccc','ddd','eee','fff','ggg']
        })
        inp_df.set_index('idx', inplace=True)

        inp_comp_df = inp_df.assign(**{
            'col2': ['abc','def','xyz','hij','lmn','xxx', None],
            'col2_xxx': ['abc', 'def', 'xyz', 'hij', 'lmn', 'xxx', None],
            'newcol_pat': ['xxx', 'yyy', 'zzz', 'rrr', 'www', 'ttt', 'fff']
        })

        exp_df = inp_df.assign(**{
            'is_diff': [False, False, False, False, False, False, False]
        })

        got_df = df_utils.diff_rows(
            inp_df, inp_comp_df, out_col='is_diff',
            exclude_col_patterns=['col2','newcol']
        )
        pd.testing.assert_frame_equal(exp_df, got_df)

    def test_diff_rows_exclude_col(self):
        inp_df = pd.DataFrame(data={
            'idx': ['a', 'b', 'c', 'd', 'e', 'f', 'g'],
            'col1': [1, 2, 3, 4, 5, 6, 7],
            'col2': ['abc', 'def', 'xyz', 'hij', 'lmn', 'qrs', 'tuv'],
            'col2_xxx': ['abc', 'def', 'xyz', 'hij', 'lmn', 'qrs', 'tuv'],
        })
        inp_df.set_index('idx', inplace=True)

        inp_comp_df = inp_df.assign(**{
            'col2': ['abc', 'def', 'xyz', 'hij', 'lmn', 'xxx', None],
            'col2_xxx': ['abc', 'def', 'xyz', 'hij', 'lmn', 'xxx', None]
        })

        exp_df = inp_df.assign(**{
            'is_diff': [False, False, False, False, False, False, False]
        })

        got_df = df_utils.diff_rows(
            inp_df, inp_comp_df, out_col='is_diff',
            exclude_col_patterns=['col2']
        )
        pd.testing.assert_frame_equal(exp_df, got_df)

if __name__ == '__main__':
    unittest.main()