__author__ = 'arash'

import copy
import logging
import asyncio
import ssl
import pickle
from message_types import ackknowledgment
from message_types import measurement_msg
from message_types import requests


_logger = logging.getLogger(__name__)


def create_ssl_context(certfile, keyfile, root_pem):
    """
    Creates and returns a ssl.SSLContext
    certfile and root_pem must be in PEM format
    :param certfile: path to  the certification file
    :param keyfile: path to the key
    :param root_pem: path to the root certification file
    :return: ssl.SSLContext
    """
    sslcontext = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
    sslcontext.load_cert_chain(certfile=certfile, keyfile=keyfile)
    sslcontext.load_verify_locations(root_pem)
    sslcontext.verify_mode = ssl.CERT_REQUIRED
    return sslcontext


class Server():
    """
    a class for communication through ssl on NonBlocking IO
    receives measurement data from clients and pushes them into
    shared queues to be consumed by other classes
    """

    def __init__(self, sslcontext, storage_queue, carbon_queue, expireed_certs, host="localhost", port=1234):
        self._host = host
        self._port = port
        self._sslcontext = sslcontext
        self._server = None
        self._data = None

        # asyncio loop
        self._loop = None
        self._storage_queue = storage_queue
        self._carbon_queue = carbon_queue

        # a list containing serial numbers of expired certificates
        self._expired_certs = expireed_certs

        # parameters for checking the messages
        # which have not been received
        self._not_received_list = []
        self._last_received = 0

    @asyncio.coroutine
    def client_connected(self, reader, writer):
        """ handling client connections
        :param reader:
        :param writer:
        :return:
        """

        # check if the certificate of the client has been expired!
        certDict = writer.get_extra_info("peercert")
        if certDict['serialNumber'] in self._expired_certs:
            _logger.warn("#warn:connection-from-expired-client:%s" % certDict['serialNumber'])
            writer.write('Sorry! Your certificate has been expired!'.encode())
            writer.close()
            return

        # handling messages from certified clients
        _logger.info("#info:connection-stablished#peercert:%s" % (writer.get_extra_info("socket").getpeercert()))

        while True:
            try:

                # wait to receive a message from the client
                rec = yield from reader.read(1000)
                if not rec:
                    break

                # analyze the message and send ack
                data = yield from self.analyze_msg(rec, writer)

                # the message received is useful
                # push it into queues
                if data:
                    self.push_into_queues(data)
            except KeyboardInterrupt:
                return
            except Exception as e:
                _logger.error("#error:error-while-handling-msg:%s" % rec)
                _logger.debug("#debug:size-of-msg:%s" % len(rec))
                _logger.exception(e)
                continue

    @asyncio.coroutine
    def analyze_msg(self, byte_msg, writer):
        """
        Analyzes a message received from the client to see
        if its a new message, a wanted message, or already seen message
        sends an acknowledgment if necessary
        :param byte_msg: (bytes)
        :param writer:
        :return: (list) the content of the message, which is a list
        """
        try:
            msg = pickle.loads(byte_msg)

            # msg must be subclass of GeneralMessage
            if msg.get_type() == 'measurement':
                return self.handle_measurement_msg(msg, writer)
            elif msg.get_type() == 'request':
                return self.handle_request(msg)
            else:
                _logger.warn("#warn:unexpected-message-type%s" % msg.get_type())
        except pickle.UnpicklingError as e:
            _logger.error("#error:while-unpickling-msg-size:%s" % len(byte_msg))
            _logger.debug("#debug:problematic-msg:%s" % str(byte_msg))
            _logger.exception(e)
            return None
        except KeyError as e:
            _logger.error("#error:corrupted-msg-%s" % msg)
        except AttributeError as e:
            _logger.error("#error:message-is-corrupted-%s" % msg)

    def handle_measurement_msg(self, msg, writer):

        try:
            msg_id = msg.get_id()

            # check if we miss some messages
            # since the last msg received
            if msg_id > self._last_received:
                for i in range(self._last_received+1, msg_id):
                    self._not_received_list.append(i)

                _logger.debug("#debug:not-received_list-updated:%s" % self._not_received_list)

                # updating last_received msg id
                self._last_received = msg_id

                # sending acknowledgment
                yield from self.send_ack(msg_id, writer)
                return msg.get_data()

            # a requested message arrived! remove it
            # from the wanted list
            elif msg_id < self._last_received:
                if msg_id in self._not_received_list:

                    # The client send an empty data with msg_id
                    # which means the client does not have that
                    # msg anymore
                    if not msg.get_data():
                        _logger.warn("#warn:msg:%s-is-completely-lost!-client-sent-None" % msg_id)

                        # removing from wanted list
                        self._not_received_list.remove(msg_id)
                        return None

                    else:
                        self._not_received_list.remove(msg_id)
                        _logger.debug("#debug:Finally!-received-msg-with-id: %s" % msg_id)

                        # sending acknowledgment
                        yield from self.send_ack(msg_id, writer)
                        return msg.get_data()

                else:
                    _logger.debug("#debig:msg_id:%s:-is-<-last_received-but-not-in-not_received-list" % msg_id)
                    # requesting to get msg counter of the client
                    yield from self.send_request(writer)

        except KeyError as e:
            _logger.warn("#warn:corrupted-message!")
            _logger.exception(e)
            return None

    def handle_request(self, msg):
        if msg.get_request() == 'GET_MSG_COUNTER':
            self._last_received = msg.get_response() - 1
        else:
            _logger.debug("#debug:unknown-request-%s: " % msg.get_request())

    @asyncio.coroutine
    def send_ack(self, msg_id, writer):
        """
        sends an acknowledgment to the client
        :param msg_id: (int) the message that has been successfully received
        :param writer:
        """

        # check if there is a message which server missed
        wanted = None
        if len(self._not_received_list) > 0:
            wanted = self._not_received_list[0]

        # creating and sending the acknowledgment message
        ack = ackknowledgment.Acknowledgment(msg_id, wanted)
        _logger.debug("#debug:sending-ack-%s" % ack)
        byte_ack = pickle.dumps(ack)
        writer.write(byte_ack)
        yield from writer.drain()

    @asyncio.coroutine
    def send_request(self, writer, request='GET_MSG_COUNTER'):
        req = requests.Request(request=request, data=None)
        _logger.debug("#debug:sending-request-%s" % req)
        byte_req = pickle.dumps(req)
        writer.write(byte_req)
        yield from writer.drain()

    def push_into_queues(self, data):
        """
        pushes the items inside the data into shared queues
        :param data: list of dictionaries (measurements)
        """
        for item in data:
            self._storage_queue.put(copy.copy(item))
            self._carbon_queue.put(copy.copy(item))

    def run(self):
        _logger.info('#info:starting-the-server-on-port-%s' % self._port)
        self._loop = asyncio.get_event_loop()
        coro = asyncio.start_server(self.client_connected, self._host, self._port, ssl=self._sslcontext)
        self._server = self._loop.run_until_complete(coro)
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            _logger.error("#error:server-stopped-with-keyboardInterrupt")
            pass
        except Exception as e:
            _logger.error("#error:unpredicted-exception-in-server")
            _logger.exception(e)
        finally:
            self.disconnect()

    def disconnect(self):
        _logger.info("#info:disconnecting-the-server")
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())
        self._loop.close()
