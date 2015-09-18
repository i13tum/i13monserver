
from message_types import general_message

class MeasurementMessage(general_message.GeneralMessage):
	"""
	A class for measurement messages which its content is a dictionary
	{'type'='measurement', 'id': (int) msg_id, 'data': list_of_dictionaries (measurements)}
	"""
	def __init__(self, id, data):
		super().__init__()
		self._content['type'] = 'measurement'
		self.set_data(data)
		self.set_id(id)

	def get_id(self):
		return self._content['id']

	def get_data(self):
		return self._content['data']

	def set_id(self, id):
		self._content['id'] = id

	def set_data(self, data):
		self._content['data'] = data