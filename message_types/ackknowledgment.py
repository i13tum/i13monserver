
from message_types import general_message

class Acknowledgment(general_message.GeneralMessage):
	"""
	A class for Acknowledgment messages
	content = {'type':'ack', 'success': (int) msg_id, 'wanted': (int) msg_id}
	"""
	def __init__(self, succes_id, wanted_id):
		super().__init__()
		self._content['type'] = 'ack'
		self.set_success(succes_id)
		self.set_wanted(wanted_id)

	def get_success(self):
		return self._content['success']

	def get_wanted(self):
		return self._content['wanted']

	def set_success(self, id):
		self._content['success'] = id

	def set_wanted(self, id):
		self._content['wanted'] = id
