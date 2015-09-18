import logging
import socket
import time
import multiprocessing
import os
from util import utilities

# default carbon service configuration
_CARBON_HOST = '0.0.0.0'
_CARBON_PORT = 2003


"""
The retention rate which has been set in carbon's storage-schema.conf
for our schema: in this case 'i13mon.*'
after sending power-measurements we sleep 1 second, otherwise carbon will
not store the next data which is sent in the same time frame of 1 second
(power-measurement are sent twice per second)
"""
_RETENTION_RATE = 1

_logger = logging.getLogger(__name__)


def load_namespace_dict(filename='namespace.csv'):
	"""
	loads the csv file containing the mapping for deviceid/mc_address to hierarchical names
	:param filename:
	:return: dictionary
	"""
	name_dict = {}
	with os.open(filename, mode='r+') as fin:
		for line in fin.readlines():
			id_name_pair = line.split(',')
			name_dict[id_name_pair[0]] = id_name_pair[1].strip()
	return name_dict

class CarbonClient(multiprocessing.Process):
	"""
	The main Class responsible for sending data to the
	carbon storage service
	"""
	def __init__(self, queue, name_dict):
		"""
		:param queue: the shared queue for consuming data
		:param name_dict: a dictionary containing mapping for device's name
		and deviceid/mac_address
		:return:
		"""
		multiprocessing.Process.__init__(self, daemon=True)
		self._socket = socket.socket()
		self.queue = queue
		self.dictionary = name_dict

	def connect(self):
		self._socket.connect((_CARBON_HOST, _CARBON_PORT))
		_logger.debug('#debug:connected!')

	def disconnect(self):
		self._socket.close()

	def get_message_filter(self, data):
		"""
		cheeck to see if there is a mapping for this device in file name_dict
		:param data: dictionary representing a node (a measurement)
		:return: string : hierarchical name for the device or deviceid/mcAddress if
		no hierarchical name is mapped for this device
		"""
		if data['type'] == 'power_measurement' or data['type'] == 'temp_hum_measurement' :
			if str(data['deviceid']) in self.dictionary:
				return self.dictionary[str(data['deviceid'])]
			else:
				_logger.warn('#debug:no-name-exists-for-device:-%s' % data['deviceid'])
				return data['deviceid']
		elif data['type'] == 'plug_measurement':
			if str(data['mac_address']) in self.dictionary:
				return self.dictionary[str(data['mac_address'])]
			else:
				_logger.warn('#debug:no-name-exists-for-device:-%s' % data['mac_address'])
				return data['mac_address']

	def send_power_measurement(self, data, message_filter):
		_logger.debug('#debug:sending-power=measurement-' + message_filter)
		self.send(message_filter+'power1', data['power1'])
		self.send(message_filter+'power2', data['power2'])
		self.send(message_filter+'power3', data['power3'])
		self.send(message_filter+'power4', data['power4'])
		self.send(message_filter+'vrms', data['vrms'])
		self.send(message_filter+'temperature', data['temp'])

	def send_temp_hum_measurement(self, data, message_filter):
		_logger.debug('#debug:sending-temp-hum-measurement' + message_filter)
		self.send(message_filter+'temperature', data['temp'])
		self.send(message_filter+'external_temperature', data['temp_external'])
		self.send(message_filter+'humidity', data['humidity'])
		self.send(message_filter+'battery', data['battery'])

	def send_plug_measurement(self, data, message_filter):

		# check to see if the data is worthy of storing!
		# in order to scape storing Null values, when Zigbee
		# is shut down and send just POW=OFF in case of load overload
		if utilities.check_plug_measurement(data):
			_logger.debug('#debug:sending-plug=measurement-' + message_filter)
			self.send(message_filter+'load', data['load'])
			self.send(message_filter+'work', data['work'])
			self.send(message_filter+'power', data['pow'])
			self.send(message_filter+'frequency', data['freq'])
			self.send(message_filter+'vrms', data['vrms'])
			self.send(message_filter+'irms', data['irms'])
		else:
			# TODO send an email?
			_logger.error("#error-the-zigbee-device-has-been-turned-off-%s " % data)


	def send(self, message_filter, data):
		"""
		send a measure to the carbon storage service
		:param message_filter: hierarchical name of the measurement: eg :i13mon.kitchen.rmpi.power1
		:param data: the value of that measurement
		:return:
		"""
		# appending a timestamp to the message and send it to carbon
		msg = "%s %s %d\n" % (message_filter, data, int(time.time()))
		self._socket.sendall(msg.encode('ascii'))

	def run(self):
		while True:
			try:
				data = self.queue.get()
				# _logger.debug("debug:-data-%s" % data)

				# get a general path for the metrics which we want to send
				message_filter = self.get_message_filter(data)
				ty = data['type']
				if ty == 'power_measurement':
					self.send_power_measurement(data, message_filter)
					time.sleep(_RETENTION_RATE)
				elif ty == 'plug_measurement':
					self.send_plug_measurement(data, message_filter)
					pass
				elif ty == 'temp_hum_measurement':
					self.send_temp_hum_measurement(data, message_filter)
				else:
					_logger.warn("#warn:unknown-datatype!-%s" % data)
			except ConnectionError as e:

				# Connection with the carbon webservice is lost!
				_logger.error("#error:disconnected-from-carbon-webservice")
				_logger.exception(e)
				time.sleep(2)
				self.connect()
				self.run()
			except Exception as e:
				_logger.error("#error:Unknown-exception-in-carbon-agent")
				_logger.exception(e)
				time.sleep(2)
				self.run()
