## Amazon S3 manager
## Author: Michal Ludvig <michal@logix.cz>
##         http://www.logix.cz/michal
## License: GPL Version 2

import re
import sys
from BidirMap import BidirMap
from logging import debug
from S3 import S3
from Utils import unicodise, deunicodise

class S3Uri(object):
	type = None
	_subclasses = None

	def __new__(self, string):
		if not S3Uri._subclasses:
			## Generate a list of all subclasses of S3Uri
			S3Uri._subclasses = []
			dict = sys.modules[__name__].__dict__
			for something in dict:
				if type(dict[something]) is not type(self):
					continue
				if issubclass(dict[something], S3Uri) and dict[something] != S3Uri:
					S3Uri._subclasses.append(dict[something])
		for subclass in S3Uri._subclasses:
			try:
				instance = object.__new__(subclass)
				instance.__init__(string)
				return instance
			except ValueError, e:
				continue
		raise ValueError("%s: not a recognized URI" % string)
	
	def __str__(self):
		return self.uri()

	def __unicode__(self):
		return self.uri()

	def public_url(self):
		raise ValueError("This S3 URI does not have Anonymous URL representation")

	def basename(self):
		return self.__unicode__().split("/")[-1]

class S3UriS3(S3Uri):
	type = "s3"
	_re = re.compile("^s3://([^/]+)/?(.*)", re.IGNORECASE)
	def __init__(self, string):
		match = self._re.match(string)
		if not match:
			raise ValueError("%s: not a S3 URI" % string)
		groups = match.groups()
		self._bucket = groups[0]
		self._object = unicodise(groups[1])

	def bucket(self):
		return self._bucket

	def object(self):
		return self._object
	
	def has_bucket(self):
		return bool(self._bucket)

	def has_object(self):
		return bool(self._object)

	def uri(self):
		return "/".join(["s3:/", self._bucket, self._object])
	
	def public_url(self):
		if S3.check_bucket_name_dns_conformity(self._bucket):
			return "http://%s.s3.amazonaws.com/%s" % (self._bucket, self._object)
		else:
			return "http://s3.amazonaws.com/%s/%s" % (self._bucket, self._object)

	@staticmethod
	def compose_uri(bucket, object = ""):
		return "s3://%s/%s" % (bucket, object)

class S3UriS3FS(S3Uri):
	type = "s3fs"
	_re = re.compile("^s3fs://([^/]*)/?(.*)", re.IGNORECASE)
	def __init__(self, string):
		match = self._re.match(string)
		if not match:
			raise ValueError("%s: not a S3fs URI" % string)
		groups = match.groups()
		self._fsname = groups[0]
		self._path = unicodise(groups[1]).split("/")

	def fsname(self):
		return self._fsname

	def path(self):
		return "/".join(self._path)

	def uri(self):
		return "/".join(["s3fs:/", self._fsname, self.path()])

class S3UriFile(S3Uri):
	type = "file"
	_re = re.compile("^(\w+://)?(.*)")

	def __init__(self, path):
		# path - should contain unicode version of path
		# rawpath - should contain byte-stream path, ie not unicode
		# on a typical UTF-8 system: rawpath=path.encode("utf-8")
		match = self._re.match(path)
		groups = match.groups()
		if groups[0] not in (None, "file://"):
			raise ValueError("%s: not a file:// URI" % path)
		path = groups[1]
		if rawpath is not None:
			self._rawpath = rawpath
		else:
			if type(path) != unicode:
				self._rawpath = path
			else:
				self._rawpath = deunicodise(path)
		if type(path) != unicode:
			path = unicodise(path)
		self._path = path

	def set_rawpath(self, rawpath):
		"""
		set_rawpath(rawpath)
		override byte-stream representation of unicode 'self.path()'
		On a typical UTF-8 system: rawpath=path.encode("utf-8")
		"""
		assert(type(rawpath) = str)
		self._rawpath = rawpath

	def path(self):
		"returns <type 'unicode'>"
		return self._path

	def rawpath(self):
		"""
		returns path encoded according to current
		system encoding, <type 'str'>
		
		"""
		return self._rawpath

	def uri(self):
		return "/".join(["file:/", self.path()])

	def __str__(self):
		return self.rawpath()

	def __unicode__(self):
		return self.path()

if __name__ == "__main__":
	uri = S3Uri("s3://bucket/object")
	print "type()  =", type(uri)
	print "uri     =", uri
	print "uri.type=", uri.type
	print "bucket  =", uri.bucket()
	print "object  =", uri.object()
	print

	uri = S3Uri("s3://bucket")
	print "type()  =", type(uri)
	print "uri     =", uri
	print "uri.type=", uri.type
	print "bucket  =", uri.bucket()
	print

	uri = S3Uri("s3fs://filesystem1/path/to/remote/file.txt")
	print "type()  =", type(uri)
	print "uri     =", uri
	print "uri.type=", uri.type
	print "path    =", uri.path()
	print

	uri = S3Uri("/path/to/local/file.txt")
	print "type()  =", type(uri)
	print "uri     =", uri
	print "uri.type=", uri.type
	print "path    =", uri.path()
	print
