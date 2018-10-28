import twython
from lad.handlers.static import twitter_configs

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

    def search(self, q, result_type='recent', count=100, lang='en'):
        return self.twython.search(
            q=q,
            result_type=result_type,
            count=count,
            lang=lang
        )

