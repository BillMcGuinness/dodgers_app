import lad
import optparse
import pandas as pd
from jobs.twitter.dodgers_twitter_lib import (
    get_since_id, process_df, add_etl_cols
)

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

def run_job():
    #instantiate our handler
    twitter = lad.TwitterHandler('LA_APP')
    #find id of earliest tweet we are interested in
    since_id = get_since_id(twitter, _DEST_TABLE_MAP.get('tweet_df'), _SEARCH_STRING)

    max_id = None #to start, we don't want a max_id--give us the most recent
    done = False
    search_num = 1
    # since standard twitter search only returns 100 tweets at a time, we need to loop to get all tweets we want
    while not done:
        print('Starting search number {}'.format(search_num))
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
                # this is required because we search backwards in time, and I wouldn't want to update a record with
                # older data.  Probably a better way to handle this, maybe by starting the search as far back in time
                # and proceeding forward
                update_with_older_data_col = 'update_with_older_data'
            #add etl cols
            df = add_etl_cols(df)

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
            # hard coding this...will break if next_results format changes!!!
            next_results_substr_start = next_results.find('max_id=') + 7
            next_results_substr_end = next_results.find('&q=')
            max_id = next_results[
                next_results_substr_start:next_results_substr_end
            ]
            search_num += 1
        else:
            done = True


def process_command_line():
    optp = optparse.OptionParser()
    optp.add_option(
        '--env', dest='environment', help='Environment', default='DEV',
    )
    optp.add_option(
        '--log_file', dest='log_file', help='Log File',
    )
    opts, args = optp.parse_args()

    return opts

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    print('Starting job')
    opts = process_command_line()
    try:
        run_job()

    except Exception as e:
        # job email reporter to let us know it failed
        raise e