import lad
from datetime import datetime, timedelta
import getpass
import pandas as pd
import logging

log = logging.getLogger()
log.setLevel(logging.INFO)

_SEARCH_STRING = """
    from:Dodgers OR #Dodgers OR #BleedBlue OR #Doyers OR #LADodgers
"""

_DEST_TABLE_MAP = {
    'search_df': 'dbo.searches',
    'tweet_df': 'dbo.tweets',
    'user_df': 'dbo.users',
    'user_mention_df': 'dbo.users',
    'tweet_user_mention_df': 'dbo.tweet_user_mention',
    'media_df': 'dbo.media',
    'tweet_media_df': 'dbo.tweet_media',
    'places_df': 'dbo.places',
    'places_coordinates_df': 'dbo.places_coordinates'
}

def find_youngest_tweet_older_than(twitter, older_than_hours=2):
    max_id = None
    older_than_limit = str(datetime.utcnow() - timedelta(
        hours=older_than_hours))
    while True:
        df_map = twitter.search_to_dfs(
            _SEARCH_STRING, max_id=max_id, result_type='recent'
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

def run_job():
    #instantiate our handler
    twitter = lad.TwitterHandler('LA_APP')

    # check to see if we have any tweets in the DB already
    with lad.DbHandler('LA_DB') as db:
        min_db_tweet_id = db.sql_to_df(
            query="""
                SELECT MIN(t.id)
                FROM {} t
            """.format(_DEST_TABLE_MAP.get('tweet_df'))
        ).iloc[0,0]

    if not min_db_tweet_id:
        print('No tweets in db, searching for since_id')
        # if we don't have any tweets in our table, we need to find the
        # youngest tweet older than 2 hours
        since_id = find_youngest_tweet_older_than(twitter, 2)
    else:
        # for this job, I want to get updates to tweets we've already stored,
        #  so since_id is smallest tweet ID we have
        print('Found tweets in db, using oldest as since_id')
        since_id = min_db_tweet_id

    #to start, we don't want a max_id--give us the most recent
    max_id = None
    done = False
    while not done:
        df_map = twitter.search_to_dfs(
            q=_SEARCH_STRING, since_id=since_id, max_id=max_id,
            result_type='recent'
        )
        tweet_df = df_map['tweet_df']

        if max_id and tweet_df.empty:
            # if we've set a max ID and received no tweets back, it means we
            # had to loop through more than 1 search.  I don't feel it is
            # necessary to store this search in the DB.  If, however, we
            #  received no tweets back and HAVEN'T set max_id, it means it
            # was a "first search" and I do think we should log this search
            # in the db
            done = True
            return

        for k, df in df_map.items():
            update_if_diff = True
            if k == 'user_mention_df':
                #since the user_mention data is only a few fields (id, name,
                # screename), I don't want to update the user data we already
                #  have
                update_if_diff = False
            update_with_older_data_col = None
            if k == 'user_df':
                update_with_older_data_col = 'update_with_older_data'
            #add etl cols
            now_date = str(datetime.now())
            user = getpass.getuser()
            df['etl_created_date'] = now_date
            df['etl_modified_date'] = now_date
            df['etl_created_by'] = user
            df['etl_modified_by'] = user
            df['job_name'] = __file__

            dest_table = _DEST_TABLE_MAP.get(k)
            audit_table = dest_table + '_HIST'
            print('Processing {0}: {1}'.format(k, str(df.shape)))
            process_df(
                df, dest_table, update_if_diff, audit_table=audit_table, max_id=max_id,
                update_with_older_data_col=update_with_older_data_col
            )

        # after processing this current search's results, get max_id from
        # seach metadata next_results parameters and keep searching further
        # back in the past
        search_df = df_map['search_df']
        next_results = search_df['next_results'].iloc[0]
        if next_results:
            # hard coding this...will break if next_results format changes
            next_results_substr_start = next_results.find('max_id=') + 7
            next_results_substr_end = next_results.find('&q=')
            max_id = next_results[
                next_results_substr_start:next_results_substr_end
            ]
        else:
            done = True


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
            cols_to_not_overwrite = [
                'etl_created_date','etl_created_by'
            ]
            audit_df = diff_df.copy()
            # update new data with cols from old data we want to preserve
            diff_df.update(updateable_existing_data[cols_to_not_overwrite])
            ids_to_delete = [(i,) for i in diff_df.index]
            # update

            # db.delete_rows(
            #     table=dest_table.split('.')[1],
            #     schema=dest_table.split('.')[0], table_cols=['id'],
            #     delete_conds=ids_to_delete
            # )
            diff_df.drop(out_col, axis=1, inplace=True)
            diff_df.reset_index(inplace=True)
            # db.df_to_table(
            #     diff_df, table=dest_table.split('.')[1],
            #     schema=dest_table.split('.')[0], append=True
            # )
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



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    print('Starting job')
    run_job()