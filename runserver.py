import logging
import multiprocessing
from dbmnger import database_manager
from server import Server, create_ssl_context
from util import cfg
from util.logger_factory import setup_logging
from carbon_mnger import carbon_agent

_logger = logging.getLogger(__name__)


def create_ssl_server(storage_queue, carbon_queue):
    """
    Creates and returns an instance of a Server communicating in secure channel
    :param storage_queue: (multiprocessing.Queue) shared queue between server and DatabaseManager
    :param carbon_queue: (multiprocessing.Queue) shared queue between server and CarbonAgent
    :return: an instance of Server
    """

    # loading the configuration
    com_config = cfg.get_server_config()
    ssl_config = cfg.get_ssl_config()
    expired_certs = cfg.get_expired_certificates()["serialNumbers"].split(";")
    try:
        if com_config and ssl_config:
            host = com_config["serverAddress"]
            port = int(com_config["serverPort"])

            certfile = ssl_config["certFile"]
            keyfile = ssl_config["keyFile"]
            root_pem = ssl_config["locationVerification"]
            sslctx = create_ssl_context(certfile, keyfile, root_pem)

            server = Server(sslctx, storage_queue, carbon_queue, expired_certs, host, port)

            return server
    except Exception as e:
        _logger.error("Configuration failed!", e)


def create_postgres_connection():
    """
    Creates and returns a connection to the Postgres Database
    """
    # loading database configuration
    dbcfg = cfg.get_db_config()
    return database_manager.create_connection(dbcfg['dbname'], dbcfg['user'], dbcfg['password'], dbcfg['host'], dbcfg['port'])


if __name__ == '__main__':
    setup_logging()

    storage_queue = multiprocessing.Queue(maxsize=0)
    carbon_queue = multiprocessing.Queue(maxsize=0)

    # setting and starting the DatabaseManager process
    postgresconnection = create_postgres_connection()
    dbmanager = database_manager.DatabaseManager(queue=storage_queue, db_connection=postgresconnection)
    dbmanager.start()

    # setting and starting the CarbonAgent process
    name_dictionary = carbon_agent.load_namespace_dict()
    c = carbon_agent.CarbonClient(carbon_queue, name_dictionary)
    c.connect()
    c.start()

    # setting and starting the SSLServer
    s = create_ssl_server(storage_queue, carbon_queue)
    s.run()
