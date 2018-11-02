import uuid
from functools import partial
import pandas as pd
import re

def create_row_id(df_row, out_col):
    df_row_list = df_row.tolist()
    df_row_str_list = [str(i) for i in df_row_list]
    row_str = ';;;'.join(df_row_str_list)
    df_row[out_col] = str(uuid.uuid5(uuid.NAMESPACE_DNS, row_str))
    return df_row

def create_df_id(df, out_col):
    return df.apply(partial(create_row_id, out_col=out_col), axis=1)

def diff_rows(df, compare_df, out_col, exclude_col_patterns=None):
    df[out_col] = None

    overlapping_cols = set(df.columns).intersection(set(compare_df.columns))

    if exclude_col_patterns:
        for exclude_col_pattern in exclude_col_patterns:
            overlapping_cols = [
                col for col in overlapping_cols
                if not re.search(exclude_col_pattern, col)
            ]

    def _diff_row(row):
        row_idx = row.name
        if row_idx in compare_df.index:
            compare_row = compare_df.loc[row_idx,:]
        else:
            return row
        if isinstance(compare_row, pd.DataFrame):
            raise Exception(
                'compare df has more than 1 row for index: {}'.format(
                    str(row_idx)
                )
            )
        row[out_col] = False
        for overlapping_col in overlapping_cols:
            if row[overlapping_col] != compare_row[overlapping_col]:
                row[out_col] = True
                return row
            else:
                continue

        return row

    return df.apply(_diff_row, axis=1)