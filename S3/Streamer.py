## Amazon S3 manager
## Author: Michal Ludvig <michal@logix.cz>
##         http://www.logix.cz/michal
## License: GPL Version 2

import StringIO

## Make new-style class from StringIO
class StringIO(StringIO.StringIO, object):
	pass

class Streamer(object):
	def __init__(self, stream):
		try:
			stream.__getattribute__
		except AttributeError, e:
			raise ValueError("%s requires new-style class, but %s is old-style class." % (
				self.__class__.__name__, stream.__class__.__name__))
		self._stream = stream
		self.eof = False
	
	def read(self, amount = None):
		data = self._stream.read(amount)
		if len(data) == 0:
			self.eof = True
			raise EOFError
		return data

	def __getattribute__(self, attribute):
		try:
			return object.__getattribute__(self, attribute)
		except AttributeError:
			pass
		return self._stream.__getattribute__(attribute)


