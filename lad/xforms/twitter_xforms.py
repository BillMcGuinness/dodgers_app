import pandas as pd


def get_tweet_df(search_res):
    tweets = search_res['statuses']

    if len(tweets) == 0:
        #todo something with empty res
        pass

    tweet_df = pd.DataFrame()

    for tweet in tweets:
        tweet_row = pd.DataFrame(
            data={
                'coordinates': [tweet['coordinates']],
                'created_at': [tweet['created_at']],
                'favorite_count': [tweet['favorite_count']],
                'favorited': [tweet['favorited']],
                # geo': [tweet['geo']],
                'id': [tweet['id_str']],
                # 'in_reply_to_screen_name': [tweet[
                                  # 'in_reply_to_screen_name']],
                # 'in_reply_to_status_id': [tweet[
                                  # 'in_reply_to_status_id_str']],
                # 'in_reply_to_user_id': [tweet[
                                  # 'in_reply_to_user_id_str']],
                'is_quote_status': [tweet['is_quote_status']],
                'lang': [tweet['lang']],
                # 'place': [tweet['place']],
                'retweet_count': [tweet['coordinates']],
                'retweeted': [tweet['retweeted']],
                'source': [tweet['source']],
                'text': [tweet['text']],
                'truncated': [tweet['truncated']],
            }
        )

        tweet_df = pd.concat([tweet_df, tweet_row])

    return tweet_df