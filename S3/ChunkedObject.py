import os

from S3 import *
from S3Uri import *
from Streamer import *

class ChunkedObjectFile(object):
	def __init__(self, prefix, max_size, autoremove = True):
		self._max_size = max_size
		if os.path.isdir(prefix):
			if not prefix.endswith(os.sep):
				prefix += os.sep
		self._f_name = mktmpfile(prefix)
		self._f = open(self._f_name, "w")
		self._autoremove = autoremove

	def free_space(self):
		return self._max_size - self._f.tell()

	def size(self):
		return self._f.tell()

	def append(self, data):
		if len(data) <= self.free_space():
			_data = data
			rest = None
		else:
			_data = data[:self.free_space()]
			rest = data[self.free_space():]
		self._f.write(_data)
		return rest

	def close(self):
		if not self._f.closed:
			self._f.close()

	def name(self):
		return self._f_name

	def md5(self):
		return "NotYetMD5"

	def sha1(self):
		return "NotYesySHA1"

	def set_max_size(self, new_size):
		if new_size < self._f.tell():
			raise ValueError("%s.set_max_size(%d) to value smaller than current file size %d" % (
				new_size, self._f.tell()))
		self._max_size = new_size
	
	def __del__(self):
		if self._autoremove:
			if os.path.exists(self._f_name):
				print "Removing %s" % self._f_name
				os.unlink(self._f_name)
			else:
				print "%s does no longer exist" % self._f_name

class ChunkedObject(object):
	MODE_PUT = 0x1
	MODE_GET = 0x2

	_chunk_size = 1024
	_read_chunk_size = 128	## -> 4096 after testing!

	def __init__(self, s3uri, mode, tmp_dir = "/tmp"):
		self._tmp_dir = mktmpdir(tmp_dir + "/s3cmdChunk")
		self._uri = S3Uri(s3uri)
		self._chunklist = []
		if mode[0] == "r":
			self._mode = self.MODE_GET
		elif mode[0] == "w":
			self._mode = self.MODE_PUT
			self._current_chunk = ChunkedObjectFile(self._tmp_dir, self._chunk_size, autoremove = False)
		else:
			raise ValueError("Invalid mode: %s" % (mode))
	
	def put_stream(self, stream, close_on_eof = False):
		assert(self._mode == self.MODE_PUT)
		min = lambda x, y : ((x < y) and x or y)
		s = Streamer(stream)
		data_rest = None
		while not s.eof or data_rest:
			if self._current_chunk.free_space() <= 0L:
				self.commit_current_chunk()
				self._current_chunk = ChunkedObjectFile(self._tmp_dir, self._chunk_size, autoremove = False)
			if data_rest:
				print "More data in rest: %s (%d)" % (data_rest, len(data_rest))
				data = data_rest
			else:
				try:
					data = s.read(min(self._current_chunk.free_space(), self._read_chunk_size))
				except EOFError:
					pass
			data_rest = self._current_chunk.append(data)
		if close_on_eof:
			self.commit_current_chunk()

	def commit_current_chunk(self):
		if self._current_chunk:
			self._chunklist.append({
				'name' : self._current_chunk.name(),
				'size' : self._current_chunk.size(),
				#'md5'  : self._current_chunk.md5(),
				#'sha1' : self._current_chunk.sha1(),
				})
			print "Commiting chunk %s (%d bytes)" % (
				self._chunklist[-1]["name"],
				self._chunklist[-1]["size"])
			self._current_chunk.close()
			self._current_chunk = None
	
	def commit(self):
		self.commit_current_chunk()
		print self._chunklist
	
	def set_chunk_size(self, new_size):
		self._chunk_size = new_size
		self._current_chunk.set_max_size(new_size)

if __name__ == "__main__":
	f = ChunkedObject("s3://s3py-1/chunked.file", "w")
	f.set_chunk_size(512)
	f.put_stream(open("/etc/passwd"), "w")
	f.commit()

#	f = ChunkedObject("s3://s3py-1/chunked.file", "w")
#	f.chunk_size = 128
#	f.put("/etc/passwd")
#	f.close()
#
#	f = ChunkedObject("s3://s3py-1/stdin", "w")
#	f.chunk_size = 256
#	while True:
#		chunk = sys.stdin.read(160)
#		f.append(chunk)
#		if len(chunk) < 160:
#			break
#	f.close()
#
#	f = ChunkedObject("s3://s3py-1/stdin-streamed", "w")
#	f.chunk_size = 256
#	f.tmp_dir = "/tmp"
#	f.put_stream(sys.stdin)
#	f.close()
#
#	f = ChunkedObject("s3://s3py-1/stdin", "r")
#	out = open("/tmp/stdin.out", "w")
#	size_total = 0L
#	while not f.eof:
#		data = f.read(333)
#		out.write(data)
#		size_total += len(data)
#	print "Retrieved %s bytes" % (size_total)
