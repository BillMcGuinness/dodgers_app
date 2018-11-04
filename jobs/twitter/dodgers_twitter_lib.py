import lad
from datetime import datetime, timedelta
import getpass
import pandas as pd


def find_youngest_tweet_older_than(twitter, search_string, older_than_hours=2):
    max_id = None
    older_than_limit = str(datetime.utcnow() - timedelta(
        hours=older_than_hours))
    while True:
        df_map = twitter.search_to_dfs(
            search_string, max_id=max_id, result_type='recent'
        )
        tweet_df = df_map['tweet_df']
        older_than_df = tweet_df[tweet_df['created_at'] < older_than_limit]
        if older_than_df.empty:
            # if none of the tweets are older than 2 hours, we need to keep
            # going by using max_id
            search_df = df_map['search_df']
            next_results = search_df['next_results'].iloc[0]
            if not next_results:
                # if none of the tweets in this search were posted before 2
                # hours ago, but there isn't a next_results returned, it must
                #  mean there are none from before 2 hours ago...? seems
                # wrong, so just used 1 less than the smallest ID we got
                since_id = str(int(min(older_than_df['id'])) - 1)
                return since_id
            next_results_substr_start = next_results.find('max_id=') + 7
            next_results_substr_end = next_results.find('&q=')
            max_id = next_results[
                next_results_substr_start:next_results_substr_end
            ]
        else:
            # if we do get some tweets older than 2 hours, find the max ID of
            #  those tweets and this is what we'll use as our since_id
            since_id = max(older_than_df.index)
            return since_id

def get_since_id(twitter, tweet_table, search_string):
    # check to see if we have any tweets in the DB already
    with lad.DbHandler('LA_DB') as db:
        min_db_tweet_id = db.sql_to_df(
            query="""
                   SELECT MIN(t.id)
                   FROM {} t
               """.format(tweet_table)
        ).iloc[0, 0]

    if not min_db_tweet_id:
        print('No tweets in db, searching for since_id')
        # if we don't have any tweets in our table, we need to find the
        # youngest tweet older than 2 hours
        since_id = find_youngest_tweet_older_than(twitter, search_string, 2)
    else:
        # for this job, I want to get updates to tweets we've already stored,
        #  so since_id is smallest tweet ID we have
        print('Found tweets in db, using oldest as since_id')
        since_id = min_db_tweet_id

    return since_id

def process_df(df, dest_table, update_if_diff, audit_table=None, max_id=None, update_with_older_data_col=None):
    if df.empty:
        return
    with lad.DbHandler('LA_DB') as db:
        existing_data_df = db.sql_to_df(
            query="""
                SELECT *
                FROM {0}
            """.format(dest_table),
            index_col='id'
        )
        #insert new rows to table and audit table
        new_rows_df = df[~df.index.isin(existing_data_df.index)]
        new_rows_df.reset_index(inplace=True)
        db.df_to_table(
            new_rows_df, table=dest_table.split('.')[1],
            schema=dest_table.split('.')[0], append=True
        )
        if audit_table:
            #insert new rows to audit table
            db.df_to_table(
                new_rows_df, table=audit_table.split('.')[1],
                schema=audit_table.split('.')[0], append=True
            )

        if update_if_diff:
            if max_id and update_with_older_data_col:
                # if this isn't our first search on this run, don't update newer user data with older user data
                updateable_existing_data = existing_data_df[existing_data_df[update_with_older_data_col]]
            else:
                updateable_existing_data = existing_data_df
            out_col = 'is_diff'
            #compare new data to existing data
            diff_flag_df = lad.diff_rows(
                df, updateable_existing_data, out_col=out_col,
                exclude_col_patterns=['created', 'modified', 'update_with_older', 'search_id']
            )
            # only look at new data that is different from existing data
            diff_df = diff_flag_df[diff_flag_df[out_col]]
            if diff_df.empty:
                return
            print('Updating {} rows'.format(str(len(diff_df))))
            #make a copy before we update for audit purposes
            audit_df = diff_df.copy()
            # update new data with cols from old data we want to preserve
            diff_df.update(updateable_existing_data[[
                'etl_created_date','etl_created_by'
            ]])
            # update
            diff_df.drop(out_col, axis=1, inplace=True)
            diff_df.reset_index(inplace=True)
            db.update_table(
                table=dest_table.split('.')[1], schema=dest_table.split('.')[0], update_df=diff_df,join_col='id'
            )
            if audit_table:
                #inser updated versions to audit table
                audit_df.drop(out_col, axis=1, inplace=True)
                audit_df.reset_index(inplace=True)
                db.df_to_table(
                    audit_df, table=audit_table.split('.')[1],
                    schema=audit_table.split('.')[0], append=True
                )

def add_etl_cols(df):
    now_date = str(datetime.now())
    user = getpass.getuser()
    df['etl_created_date'] = now_date
    df['etl_modified_date'] = now_date
    df['etl_created_by'] = user
    df['etl_modified_by'] = user
    df['job_name'] = __file__
    return df