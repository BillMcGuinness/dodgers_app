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
                'geo_lat': [
                    tweet.get('geo').get('coordinates',[None])[0]
                    if tweet.get('geo') else None
                ],
                'geo_long': [
                    tweet.get('geo').get('coordinates', [None, None])[1]
                    if tweet.get('geo') else None
                ],
                'geo_type': [
                    tweet.get('geo').get('type') if tweet.get('geo') else None
                ],
                'created_at': [tweet.get('created_at')],
                'favorite_count': [tweet.get('favorite_count')],
                'favorited': [tweet.get('favorited')],
                'id': [tweet.get('id_str')],
                'place_id': [
                    tweet.get('place').get('id') if tweet.get('place') else None
                ],
                'in_reply_to_screen_name': [
                    tweet.get('in_reply_to_screen_name')
                ],
                'in_reply_to_status_id': [
                    tweet.get('in_reply_to_status_id_str')
                ],
                'in_reply_to_user_id': [
                    tweet.get('in_reply_to_user_id_str')
                ],
                'is_quote_status': [tweet.get('is_quote_status')],
                'lang': [tweet.get('lang')],
                'retweet_count': [tweet.get('retweet_count')],
                'retweeted': [tweet.get('retweeted')],
                'retweeted_tweet_id': [
                    tweet.get('retweeted_status').get('id_str')
                    if tweet.get('retweeted_status') else None
                ],
                'source': [tweet.get('source')],
                'text': [tweet.get('text')],
                'truncated': [tweet.get('truncated')],
                'possibly_sensitive': [tweet.get('possibly_sensitive')],
                'user_id': [tweet.get('user').get('id_str')]
            }
        )

        tweet_df = pd.concat([tweet_df, tweet_row], ignore_index=True)

    return tweet_df


def get_user_df(search_res):
    tweets = search_res['statuses']

    user_df = pd.DataFrame()

    for tweet in tweets:
        user_row = pd.DataFrame(data={
            'id': [tweet.get('user').get('id_str')],
            'time_zone': [tweet.get('user').get('time_zone')],
            'profile_image_url': [tweet.get('user').get('profile_image_url')],
            'translator_type': [tweet.get('user').get('translator_type')],
            'profile_image_url_https': [
                tweet.get('user').get('profile_image_url_https')
            ],
            'profile_background_image_url': [
                tweet.get('user').get('profile_background_image_url')
            ],
            'profile_sidebar_fill_color': [
                tweet.get('user').get('profile_sidebar_fill_color')
            ],
            'description': [tweet.get('user').get('description')],
            'default_profile': [tweet.get('user').get('default_profile')],
            'profile_background_image_url_https': [
                tweet.get('user').get('profile_background_image_url_https')
            ],
            'favourites_count': [tweet.get('user').get('favourites_count')],
            'name': [tweet.get('user').get('name')],
            'profile_use_background_image': [
                tweet.get('user').get('profile_use_background_image')
            ],
            'id_str': [tweet.get('user').get('id_str')],
            'contributors_enabled': [tweet.get('user').get('contributors_enabled')],
            'profile_text_color': [tweet.get('user').get('profile_text_color')],
            'listed_count': [tweet.get('user').get('listed_count')],
            'notifications': [tweet.get('user').get('notifications')],
            'profile_background_tile': [
                tweet.get('user').get('profile_background_tile')
            ],
            'tweet_count': [tweet.get('user').get('statuses_count')],
            'verified': [tweet.get('user').get('verified')],
            'geo_enabled': [tweet.get('user').get('geo_enabled')],
            'screen_name': [tweet.get('user').get('screen_name')],
            'profile_sidebar_border_color': [
                tweet.get('user').get('profile_sidebar_border_color')
            ],
            'default_profile_image': [tweet.get('user').get('default_profile_image')],
            'followers_count': [tweet.get('user').get('followers_count')],
            'utc_offset': [tweet.get('user').get('utc_offset')],
            'location': [tweet.get('user').get('location')],
            'lang': [tweet.get('user').get('lang')],
            'profile_banner_url': [tweet.get('user').get('profile_banner_url')],
            'protected': [tweet.get('user').get('protected')],
            'has_extended_profile': [tweet.get('user').get('has_extended_profile')],
            'follow_request_sent': [tweet.get('user').get('follow_request_sent')],
            'url': [tweet.get('user').get('url')],
            'profile_background_color': [
                tweet.get('user').get('profile_background_color')
            ],
            'is_translator': [tweet.get('user').get('is_translator')],
            'following': [tweet.get('user').get('following')],
            'friends_count': [tweet.get('user').get('friends_count')],
            'profile_link_color': [tweet.get('user').get('profile_link_color')],
            'created_at': [tweet.get('user').get('created_at')],
            'is_translation_enabled': [tweet.get('user').get('is_translation_enabled')]
        })

        user_df = pd.concat([user_df, user_row], ignore_index=True)
    return user_df


def get_enitity_df(search_res):
    pass

def get_user_entity_df(search_res):
    pass

def get_tweet_entity_df(search_res):
    pass

def get_search_metadata(search_res):
    pass

def get_places_df(search_res):
    pass