import uuid
from functools import partial

def create_row_id(df_row, out_col):
    df_row_list = df_row.tolist()
    df_row_str_list = [str(i) for i in df_row_list]
    row_str = ';;;'.join(df_row_str_list)
    df_row[out_col] = uuid.uuid5(uuid.NAMESPACE_DNS, row_str)
    return df_row

def create_df_id(df, out_col):
    return df.apply(partial(create_row_id, out_col=out_col), axis=1)

def diff_rows(df_1, df_2, out_col):
    pass