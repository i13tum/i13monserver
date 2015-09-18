import multiprocessing
import logging
import psycopg2
import psycopg2.extras
from util import utilities

_logger = logging.getLogger(__name__)


def create_connection(dbname, user, password, host, port):
    """
    Creates and returns a connection to the Postgres Database
    :return: psycopg2.Connection object
    """
    return psycopg2.connect(dbname=dbname, user=user, host=host, port=port, password=password)

class DatabaseManager(multiprocessing.Process):
    """
    The Class responsible for storing received measurements into
    Postgres Database
    """
    def __init__(self, queue, db_connection):
        multiprocessing.Process.__init__(self, daemon=True)
        self._queue = queue
        self._db_connection = db_connection
        psycopg2.extras.register_uuid()

    def insert(self,data):
        """
        insert the data into related table of the Postgress Database
        :param data: dictionary representing a measurement of a device
        :return:
        """
        ty = data['type']
        if ty == 'power_measurement':
            _logger.debug("#debug:inserting-power-measurement")
            self.insert_power_measurement(data)
        elif ty == 'plug_measurement':
            # check if plug-measurement is worthy of storing
            if utilities.check_plug_measurement(data):
                _logger.debug("#debug:inserting-plug-measurement")
                self.insert_plug_measurement(data)
            else:
                _logger.error("#error:zigbee-device-has-been-turned-off-%s" % data)
                # TODO do something! send an email?
        elif ty == 'temp_hum_measurement':
            _logger.debug("#debug:inserting-temp-hum-measurement")
            self.insert_temp_hum_measurement(data)
        else:
            _logger.warn("#warn:unknown-data")

    def insert_power_measurement(self, data):
        with self._db_connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO rfmpi(id, deviceid, ts, power1, power2, power3, power4, vrms, temp)' +
                'VALUES(%(id)s, %(deviceid)s, %(ts)s, %(power1)s, %(power2)s, %(power3)s, %(power4)s,' +
                 '%(vrms)s, %(temp)s);', data)
        self._db_connection.commit()

    def insert_plug_measurement(self, data):
        with self._db_connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO zigbeeplugs(id, macaddress, ts, load, irms, vrms, freq, pow, work)' +
                'VALUES (%(id)s, %(mac_address)s, %(ts)s, %(load)s, %(irms)s, %(vrms)s, %(freq)s,' +
                ' %(pow)s, %(work)s);', data)
        self._db_connection.commit()

    def insert_temp_hum_measurement(self, data):
        with self._db_connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO temphum(id, deviceid, ts, temperature, externaltemp, humidity, battery)' +
                ' VALUES (%(id)s, %(deviceid)s, %(ts)s, %(temp)s, %(temp_external)s, %(humidity)s, ' +
                '%(battery)s);', data)
        self._db_connection.commit()

    def run(self):
        """
        runs the DatabaseManager process
        """
        while True:
            try:
                data = self._queue.get()
                # _logger.debug("debug:-data-%s" % data)
                self.insert(data)
            except psycopg2.IntegrityError as e:
                _logger.warn("#warn:duplicate-tuple-insertion!")
                _logger.exception(e)
                self._db_connection.rollback()
                self.run()
            except Exception as e:
                self._db_connection.rollback()
                _logger.exception(e)


