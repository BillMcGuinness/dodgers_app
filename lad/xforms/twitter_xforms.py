import pandas as pd
from datetime import datetime
from lad.utils.df_utils import create_df_id

def get_tweet_df(search_res, search_id):
    tweets = search_res['statuses']

    if len(tweets) == 0:
        #todo something with empty res
        pass

    tweet_df = pd.DataFrame()

    for tweet in tweets:
        tweet_row = pd.DataFrame(
            data={
                'search_id': search_id,
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

    user_df.drop_duplicates(subset='id', inplace=False)

    return user_df

def get_search_df(search_res):
    search_metadata = search_res.get('search_metadata')

    search_df = pd.DataFrame(data={
        'create_date': [str(datetime.now())],
        'modified_date': [str(datetime.now())],
        'completed_in': [search_metadata.get('completed_in')],
        'count': [search_metadata.get('count')],
        'max_id_str': [search_metadata.get('max_id_str')],
        'refresh_url': [search_metadata.get('refresh_url')],
        'since_id_str': [search_metadata.get('since_id_str')],
        'max_id': [search_metadata.get('max_id')],
        'query': [search_metadata.get('query')],
        'since_id': [search_metadata.get('since_id')],
        'next_results': [search_metadata.get('next_results')]
    })

    search_df = create_df_id(search_df, out_col='id')

    return search_df

def get_user_mentions_user_df(search_res):
    tweets = search_res['statuses']

    user_df = pd.DataFrame()

    for tweet in tweets:
        if len(tweet.get('entities').get('user_mentions')) > 0:
            user_mentions = tweet.get('entities').get('user_mentions')
            for user_mention in user_mentions:
                user_mention_user_row = pd.DataFrame(data={
                    'id': [user_mention.get('id_str')],
                    'screen_name': [user_mention.get('screen_name')],
                    'name': [user_mention.get('name')],
                })
                user_df = pd.concat(
                    [user_df, user_mention_user_row], ignore_index=True
                )

    user_df.drop_duplicates(subset='id', inplace=True)

    return user_df

def get_tweet_user_mention_df(search_res):
    tweets = search_res['statuses']

    tweet_user_mention_df = pd.DataFrame()

    for tweet in tweets:
        if len(tweet.get('entities').get('user_mentions')) > 0:
            user_mentions = tweet.get('entities').get('user_mentions')
            for user_mention in user_mentions:
                tweet_user_mention_row = pd.DataFrame(data={
                    'user_id': [user_mention.get('id_str')],
                    'tweet_id': [tweet.get('id_str')],
                    'start_index': [user_mention.get('indices')[0]],
                    'end_index': [user_mention.get('indices')[1]],
                })
                tweet_user_mention_df = pd.concat(
                    [tweet_user_mention_df, tweet_user_mention_row], ignore_index=True
                )

    tweet_user_mention_df = create_df_id(
        tweet_user_mention_df, 'tweet_user_mention_id'
    )

    return tweet_user_mention_df

def get_media_df(search_res):
    tweets = search_res['statuses']

    media_df = pd.DataFrame()

    for tweet in tweets:
        if len(tweet.get('entities').get('media', [])) > 0:
            media = tweet.get('entities').get('media')
            for medium in media:
                media_row = pd.DataFrame(data={
                    'id': [medium.get('id_str')],
                    'display_url': [medium.get('display_url')],
                    'expanded_url': [medium.get('expanded_url')],
                    'media_url': [medium.get('media_url')],
                    'media_url_https': [medium.get('media_url_https')],
                    'large_height': [
                        medium.get('sizes').get('large').get('h')
                    ],
                    'large_width': [
                        medium.get('sizes').get('large').get('w')
                    ],
                    'large_resize': [
                        medium.get('sizes').get('large').get('resize')
                    ],
                    'medium_height': [
                        medium.get('sizes').get('medium').get('h')
                    ],
                    'medium_width': [
                        medium.get('sizes').get('medium').get('w')
                    ],
                    'medium_resize': [
                        medium.get('sizes').get('medium').get('resize')
                    ],
                    'small_height': [
                        medium.get('sizes').get('small').get('h')
                    ],
                    'small_width': [
                        medium.get('sizes').get('small').get('w')
                    ],
                    'small_resize': [
                        medium.get('sizes').get('small').get('resize')
                    ],
                    'thumb_height': [
                        medium.get('sizes').get('thumb').get('h')
                    ],
                    'thumb_width': [
                        medium.get('sizes').get('thumb').get('w')
                    ],
                    'thumb_resize': [
                        medium.get('sizes').get('thumb').get('resize')
                    ],
                    'source_status_id': [medium.get('source_status_id_str')],
                    'source_user_id': [medium.get('source_user_id_str')],
                    'type': [medium.get('type')],
                    'url': [medium.get('url')]
                })
                media_df = pd.concat(
                    [media_df, media_row], ignore_index=True
                )

    media_df.drop_duplicates(subset='id', inplace=True)

    return media_df

def get_tweet_media_df(search_res):
    tweets = search_res['statuses']

    tweet_media_df = pd.DataFrame()

    for tweet in tweets:
        if len(tweet.get('entities').get('media', [])) > 0:
            media = tweet.get('entities').get('media')
            for medium in media:
                tweet_media_row = pd.DataFrame(data={
                    'media_id': [medium.get('id_str')],
                    'tweet_id': [tweet.get('id_str')],
                    'start_index': [medium.get('indices')[0]],
                    'end_index': [medium.get('indices')[1]],
                })
                tweet_media_df = pd.concat(
                    [tweet_media_df, tweet_media_row], ignore_index=True
                )

    tweet_media_df = create_df_id(
        tweet_media_df, 'tweet_media_id'
    )

    return tweet_media_df

def get_places_df(search_res):
    tweets = search_res['statuses']

    place_df = pd.DataFrame()

    for tweet in tweets:
        if tweet['place']:
            place_row = pd.DataFrame(data={
                'id': [tweet.get('place').get('id')],
                'country': [tweet.get('place').get('country')],
                'country_code': [tweet.get('place').get('country_code')],
                'full_name': [tweet.get('place').get('full_name')],
                'name': [tweet.get('place').get('name')],
                'place_type': [tweet.get('place').get('place_type')],
                'url': [tweet.get('place').get('url')],
                'bounding_box_type': [
                    tweet.get('place').get('bounding_box').get('type')
                    if tweet.get('place').get('bounding_box') else None
                ]
            })

            place_df = pd.concat([place_df, place_row], ignore_index=True)

    place_df.drop_duplicates(subset='id', inplace=True)

    return place_df

def get_places_coordinates_df(search_res):
    tweets = search_res['statuses']

    place_geo_df = pd.DataFrame()

    for tweet in tweets:
        if tweet['place']:
            if tweet['place']['bounding_box']:
                for coordinate in tweet.get('place').get('bounding_box').get(
                        'coordinates')[0]:
                    place_geo_row = pd.DataFrame(data={
                        'place_id': [tweet.get('place').get('id')],
                        'place_coordinate_lat': [coordinate[1]],
                        'place_coordinate_long': [coordinate[0]],
                    })

                    place_geo_df = pd.concat(
                        [place_geo_df, place_geo_row], ignore_index=True
                    )

    place_geo_df = create_df_id(
        place_geo_df, 'place_coordinate_id'
    )
    place_geo_df.drop_duplicates(subset='place_coordinate_id', inplace=True)

    return place_geo_df