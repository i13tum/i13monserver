import configparser

__author__ = 'arash'


def _get_config():
    config = configparser.ConfigParser()
    config.read('server.config')
    return config


def get_db_config():
    return _get_config()['db']


def get_server_config():
    return _get_config()['server']

def get_ssl_config():
    return _get_config()['ssl']


def get_port():
    return get_server_config()['serverPort']


def get_expired_certificates():
    return _get_config()['expired']