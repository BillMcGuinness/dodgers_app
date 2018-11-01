import lad

_SEARCH_STRING = """
    from:Dodgers OR #Dodgers OR #BleedBlue OR #Doyers OR #LADodgers
"""

_DEST_TABLE_MAP = {
    'search_df': 'dbo.zzz_searches',
    'tweet_df': 'dbo.zzz_tweets',
    'user_df': 'dbo.zzz_users',
    'user_mention_df': 'dbo.zzz_users',
    'tweet_user_mention_df': 'dbo.zzz_tweet_user_mention',
    'media_df': 'dbo.zzz_media',
    'tweet_media_df': 'dbo.zzz_tweet_media',
    'places_df': 'dbo.zzz_places',
    'places_coordinates_df': 'dbo.zzz_places_coordinates'
}

def run_job():
    twitter = lad.TwitterHandler('LA_APP')

    df_map = twitter.search_to_dfs(q=_SEARCH_STRING)

    for k, df in df_map.items():
        update_if_diff = True
        if k == 'user_mention_df':
            update_if_diff = False

        process_df(df, _DEST_TABLE_MAP.get('k'), update_if_diff)


def process_df(df, dest_table, update_if_diff):
    with lad.DbHandler('LA_DB') as db:
        existing_data_df = db.sql_to_df(
            query="""
                SELECT *
                FROM {0}
            """.format(dest_table),
            index_col='id'
        )

        new_rows_df = df[~df.index.isin(existing_data_df.index)]
        db.df_to_table(
            new_rows_df, table=dest_table.split('.')[1],
            schema=dest_table.split('.')[0], append=True
        )

        if update_if_diff:
            diff_df = lad.diff_rows(df, existing_data_df, out_col='is_diff')



if __name__ == '__main__':
    run_job()