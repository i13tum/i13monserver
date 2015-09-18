def check_plug_measurement(data):
	"""
	checks if a plug measurement includes
	None value for measures, which means the plug
	has been shut down
	:param data: a dictionary representing a plug measurement
	:return: boolean
	"""
	if not (data['load'] and data['irms'] and data['vrms'] and
			data['freq'] and data['work']) and data['pow'] == 'OFF':
			return False
	else:
		return True