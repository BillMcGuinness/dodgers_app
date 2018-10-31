import twython
from lad.handlers.static import twitter_configs
from lad.xforms.twitter_xforms import (
    get_tweet_df, get_user_df, get_search_df, get_user_mentions_user_df,
    get_tweet_user_mention_df, get_media_df, get_tweet_media_df, get_places_df,
    get_places_coordinates_df
)

class TwitterHandler(object):

    def __init__(self, config_key):
        if not hasattr(twitter_configs, config_key):
            raise Exception('[{}] db config does not exist'.format(config_key))
        _config = getattr(twitter_configs, config_key)
        self.twython = self.init_twython(_config)

    def init_twython(self, config):
        _twython = twython.Twython(
            app_key=config.get('consumer_key'),
            app_secret=config.get('consumer_secret'),
            oauth_token=config.get('access_token'),
            oauth_token_secret=config.get('access_secret')
        )

        return _twython

    def search(
        self, q, until=None, since_id=None, max_id=None, include_entities=True,
        result_type='recent', count=100, lang='en',
    ):
        # other stuff if we wanted
        return self.twython.search(
            q=q,
            result_type=result_type,
            count=count,
            lang=lang,
            until=until,
            since_id=since_id,
            max_id=max_id,
            include_entities=include_entities
        )

    def search_to_dfs(
        self, q, until=None, since_id=None, max_id=None, include_entities=True,
        result_type='recent', count=100, lang='en',
    ):
        search_res = self.search(
            q=q,
            result_type=result_type,
            count=count,
            lang=lang,
            until=until,
            since_id=since_id,
            max_id=max_id,
            include_entities=include_entities
        )
        search_df = get_search_df(search_res)
        search_id = search_df['id'].iloc[0]
        out_df_map = {
            'search_df': search_df,
            'tweet_df': get_tweet_df(search_res, search_id),
            'user_df': get_user_df(search_res),
            'user_mention_df': get_user_mentions_user_df(search_res),
            'tweet_user_mention_df': get_tweet_user_mention_df(search_res),
            'media_df': get_media_df(search_res),
            'tweet_media_df': get_tweet_media_df(search_res),
            'places_df': get_places_df(search_res),
            'places_coordinates_df': get_places_coordinates_df(search_res)
        }

        return out_df_map