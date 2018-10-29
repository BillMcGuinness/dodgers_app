import pandas as pd


def get_tweet_df(search_res):
    tweets = search_res['statuses']

    if len(tweets) == 0:
        #todo something with empty res
        pass

    #for tweet in tweets:
