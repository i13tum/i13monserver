from message_types import general_message

class Request(general_message.GeneralMessage):
	"""
	a class for requests messages between server and client in
	order to synchronize their behaviour
	currently used for GET_MSG_COUNTER requests by server
	{'type':request, 'request': 'GET_MSG_COUNTER', 'data': data}

	"""
	def __init__(self, request, data):
		super().__init__()
		self._content['type'] = 'request'
		self.set_request(request)
		self.set_response(data)

	def get_request(self):
		return self._content['request']

	def get_response(self):
		return self._content['data']

	def set_request(self, req='GET_MSG_COUNTER'):
		self._content['request'] = req

	def set_response(self, res):
		self._content['data'] = res