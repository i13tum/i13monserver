class GeneralMessage():
	"""
    a class for other message types to inherit
    """

	def __init__(self):
		self._content = {'type': 'General'}

	def get_type(self):
		return self._content['type']

	def __str__(self):
		return str(self._content)
