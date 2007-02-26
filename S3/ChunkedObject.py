## Amazon S3 manager
## Author: Michal Ludvig <michal@logix.cz>
##         http://www.logix.cz/michal
## License: GPL Version 2

import os
from Crypto.Hash import SHA

from S3 import *
from Streamer import *

class ChunkedObjectError(Exception):
	pass

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
		self._final_size = None

	def free_space(self):
		return self._max_size - self._f.tell()

	def size(self):
		return self._f.closed and self._final_size or self._f.tell()

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
		if self._final_size is None:
			self._final_size = os.stat(self._f_name).st_size

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

class ChunkedObjectWriter(object):
	_chunk_size = 1024
	_read_chunk_size = 4096

	def __init__(self, tmp_dir = "/tmp", chunk_done_callback = None, callback_arg = None):
		self._tmp_dir = mktmpdir(tmp_dir + "/s3cmdChunk")
		self._chunklist = []
		self._callback = chunk_done_callback
		self._callback_arg = callback_arg
		self._big_sha1 = SHA.new()
		self._autoremove = (self._callback is not None)
		self._current_chunk = ChunkedObjectFile(self._tmp_dir, self._chunk_size,
				autoremove = self._autoremove, big_hash = self._big_sha1)
	
	def put_stream(self, stream, close_on_eof = False):
		min = lambda x, y : ((x < y) and x or y)
		s = Streamer(stream)
		data_rest = None
		while not s.eof or data_rest:
			if self._current_chunk.free_space() <= 0L:
				self.commit_current_chunk()
				self._current_chunk = ChunkedObjectFile(self._tmp_dir, self._chunk_size,
						autoremove = self._autoremove, big_hash = self._big_sha1)
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
			else:
				self._chunklist[-1]["filename"] = self._current_chunk.name()
			self._current_chunk = None
	
	def commit(self):
		self.commit_current_chunk()
	
	def get_info(self, stream = None):
		if not stream:
			stream = StringIO()
		stream.write("big_hash: sha1=%s\n" % self.get_big_sha1())
		for chunk in self._chunklist:
			stream.write("chunk: id=%s size=%s sha1=%s\n" % (
				chunk["obj_id"], chunk["size"], chunk["sha1"]))
		stream.seek(0)
		return stream

	def get_big_sha1(self):
		return self._big_sha1.hexdigest()
	
	def set_chunk_size(self, new_size):
		self._chunk_size = new_size
		self._current_chunk.set_max_size(new_size)

class ChunkedObjectReader(object):
	_read_chunk_size = 4096

	def __init__(self, chunk_request_callback = None, callback_arg = None):
		self._callback = chunk_request_callback
		self._callback_arg = callback_arg
		self._big_sha1 = SHA.new()
		self._info_big_hash = {}
		self._info_chunks = []
	
	def get_stream(self, info_stream, output_stream):
		r_big_hash = re.compile("^big_hash: (\w+)=([0-9a-f]+)$")
		r_chunk_1 = re.compile("^chunk: ")
		r_chunk_2 = re.compile("(\w+)=([^\s]*)")
		r_comment = re.compile("^#")
		r_empty = re.compile("^\s*$")
		for line in info_stream:
			if r_comment.match(line) or r_empty.match(line):
				continue

			if r_chunk_1.match(line):
				self._info_chunks.append({})
				for match in r_chunk_2.finditer(line):
					self._info_chunks[-1][match.group(1)] = match.group(2)

			m = r_big_hash.match(line)
			if m:
				if m.group(1) != "sha1":
					raise ChunkedObjectError("Unknown 'big_hash' algorithm '%s'. Expected sha1." % m.group(1))
				self._info_big_hash[m.group(1)] = m.group(2)
				continue
		print "Chunks: %s" % self._info_chunks 
		print "BigHash: %s" % self._info_big_hash
	
if __name__ == "__main__":
	def _put_callback(arg, chunk):
		if not arg.has_key("workdir"):
			arg["workdir"] = mktmpdir("/tmp/s3cmd/CB-")
			arg["seq"] = 0
		newname = "%s/%05d" % (arg["workdir"], arg["seq"])
		arg["seq"] += 1
		os.rename(chunk.name(), newname)
		print "Renamed %s to %s (%d bytes)" % (
			chunk.name(),
			newname,
			chunk.size())
		return newname.replace("/tmp/s3cmd/", "", 1)
	
	def _get_callback(arg, id):
		print "Callback: request for '%s'" % (id)
		return Streamer(open("/tmp/s3cmd/" + id))

	if not os.path.exists("/tmp/s3cmd"):
		os.mkdir("/tmp/s3cmd")

	w = ChunkedObjectWriter(tmp_dir = "/tmp/s3cmd", chunk_done_callback = _put_callback, callback_arg = {})
	w.set_chunk_size(500)
	w.put_stream(open("/etc/passwd"))
	w.commit()
	print "# ChunkedObject info:\n" + w.get_info().read()

	r = ChunkedObjectReader(chunk_request_callback = _get_callback)
	outfile = open(mktmpfile("/tmp/s3cmd/outfile-"), "w")
	r.get_stream(w.get_info(), outfile)
	print "Saved file %s (%d bytes)" % (outfile.name, os.stat(outfile.name).st_size)

#	f = ChunkedObject("s3://s3py-1/stdin", "r")
#	out = open("/tmp/stdin.out", "w")
#	size_total = 0L
#	while not f.eof:
#		data = f.read(333)
#		out.write(data)
#		size_total += len(data)
#	print "Retrieved %s bytes" % (size_total)
