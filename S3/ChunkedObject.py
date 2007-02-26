## Amazon S3 manager
## Author: Michal Ludvig <michal@logix.cz>
##         http://www.logix.cz/michal
## License: GPL Version 2

import os
from Crypto.Hash import SHA

from S3 import *
from Streamer import *

class ChunkedObjectFile(object):
	def __init__(self, prefix, max_size, autoremove = True, big_hash = None):
		self._max_size = max_size
		if os.path.isdir(prefix):
			if not prefix.endswith(os.sep):
				prefix += os.sep
		self._f_name = mktmpfile(prefix)
		self._f = open(self._f_name, "w")
		self._autoremove = autoremove
		self._sha1 = SHA.new()
		self._big_hash = big_hash

	def free_space(self):
		return self._max_size - self._f.tell()

	def size(self):
		return self._f.closed and os.stat(self._f_name).st_size or self._f.tell()

	def append(self, data):
		if len(data) <= self.free_space():
			_data = data
			rest = None
		else:
			_data = data[:self.free_space()]
			rest = data[self.free_space():]
		self._f.write(_data)
		self._sha1.update(_data)
		if self._big_hash:
			# This hash is supposed to hold whole-file 
			# digest, not only current chunk
			self._big_hash.update(_data)
		return rest

	def close(self):
		if not self._f.closed:
			self._f.close()

	def name(self):
		return self._f_name

	def sha1(self):
		return self._sha1.hexdigest()

	def set_max_size(self, new_size):
		if new_size < self._f.tell():
			raise ValueError("%s.set_max_size(%d) to value smaller than current file size %d" % (
				new_size, self._f.tell()))
		self._max_size = new_size
	
	def __del__(self):
		if self._autoremove:
			if os.path.exists(self._f_name):
				#print "Removing %s" % self._f_name
				os.unlink(self._f_name)
			#else:
			#	print "%s does no longer exist" % self._f_name

class ChunkedObject(object):
	MODE_PUT = 0x1
	MODE_GET = 0x2

	_chunk_size = 1024
	_read_chunk_size = 128	## -> 4096 after testing!

	def __init__(self, mode, tmp_dir = "/tmp", chunk_done_callback = None, callback_arg = None):
		self._tmp_dir = mktmpdir(tmp_dir + "/s3cmdChunk")
		self._chunklist = []
		self._callback = chunk_done_callback
		self._callback_arg = callback_arg
		self._big_sha1 = SHA.new()
		self._autoremove = (self._callback is not None)
		if mode[0] == "r":
			self._mode = self.MODE_GET
		elif mode[0] == "w":
			self._mode = self.MODE_PUT
			self._current_chunk = ChunkedObjectFile(self._tmp_dir, self._chunk_size,
					autoremove = self._autoremove, big_sha1 = self._big_sha1)
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
				self._current_chunk = ChunkedObjectFile(self._tmp_dir, self._chunk_size,
						autoremove = self._autoremove, big_sha1 = self._big_sha1)
			if data_rest:
				#print "More data in rest: %s (%d)" % (data_rest, len(data_rest))
				data = data_rest
			else:
				try:
					data = s.read(min(self._current_chunk.free_space(), self._read_chunk_size))
				except EOFError:
					break
			data_rest = self._current_chunk.append(data)
		if close_on_eof:
			self.commit_current_chunk()

	def commit_current_chunk(self):
		if self._current_chunk:
			self._chunklist.append({
				'size' : self._current_chunk.size(),
				'sha1' : self._current_chunk.sha1(),
				})
			self._current_chunk.close()
			if self._callback:
				self._chunklist[-1]["obj_id"] = self._callback(self._callback_arg, self._current_chunk)
			else
				self._chunklist[-1]["filename"] = self._current_chunk.name()
			self._current_chunk = None
	
	def commit(self):
		self.commit_current_chunk()
	
	def get_info(self, stream = None):
		if not stream:
			stream = StringIO()
		stream.write("big_sha1=%s\n" % self.get_big_sha1())
		for chunk in self._chunklist:
			stream.write("id=%s	size=%s	sha1=%s\n" % (chunk["obj_id"], chunk["size"], chunk["sha1"]))
		stream.seek(0)
		return stream

	def get_big_sha1(self):
		return self._big_sha1.hexdigest()
	
	def set_chunk_size(self, new_size):
		self._chunk_size = new_size
		self._current_chunk.set_max_size(new_size)

def _put_callback(arg, chunk):
	print "CallBack(%s): %s (%d bytes)" % (
		arg,
		chunk.name(),
		chunk.size())
	return os.path.basename(chunk.name())
	
if __name__ == "__main__":
	f = ChunkedObject("w", chunk_done_callback = _put_callback, callback_arg = "Some cookie")
	f.set_chunk_size(500)
	f.put_stream(open("/etc/passwd"), "w")
	f.commit()
	print "# ChunkedObject info:\n" + f.get_info().read()

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
