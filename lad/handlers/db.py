import pyodbc
from lad.handlers.static import db_configs

class DbHandler(object):

    def __init__(self, config_key):
        if not hasattr(db_configs, config_key):
            raise Exception('[{}] db config does not exist'.format(config_key))
        _config = getattr(db_configs, config_key)
        self.server = _config.get('server')
        self.driver = _config.get('driver')
        self.database = _config.get('database')
        self.user = _config.get('user')
        self.password = _config.get('password')
        self.odbc_module = _config.get('odbc_module')

        self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.disconnect()

    def connect(self):
        if self._conn is None:
            conn_str = 'DRIVER={%s};SERVER=%s' % (self.driver, self.server)
            conn_msg = conn_str
            if self.database:
                conn_str += ';DATABASE={}'.format(self.database)
                conn_msg += ';DATABASE={}'.format(self.database)
            if self.user and self.password:
                conn_str += ';UID={};PWD={}'.format(self.user, self.password)
                conn_msg += ';UID={};PWD=****'.format(self.user)
            self._conn = pyodbc.connect(conn_str)
        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

