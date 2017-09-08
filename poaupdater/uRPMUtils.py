# Retrieve information from headers of RPM files.
# Based on RPM file format specification: http://www.rpm.org/max-rpm/s1-rpm-file-format-rpm-file-format.html

import struct
import sys
import re
from distutils.version import LooseVersion

class BufferReader:

    def __init__(self, filename):
        self.file = file(filename, "rb")
        self.buf = str()
        self.eof = False
        self.bufsize = 8096

    def read(self, n):
        if len(self.buf) < n:
            self.__readfile(n)

        rv = self.buf[:n]
        self.buf = self.buf[n:]
        return rv

    def putback(self, s):
        self.buf = s + self.buf

    def close(self):
        self.file.close()

    def __readfile(self, n):
        if self.eof:
            return

        if n > self.bufsize:
            size_to_read = n
        else:
            size_to_read = self.bufsize

        s = self.file.read(size_to_read)
        self.eof = s is None or len(s) == 0
        self.buf += s

getulong = lambda s: struct.unpack(">L", s)[0]
readulong = lambda reader: getulong(reader.read(4))


class RPMIndexItem:

    def __init__(self, data, num):
        item = data[num * 16: num * 16 + 16]
        self.tag = getulong(item[:4])
        self.type = getulong(item[4:8])
        self.off = getulong(item[8:12])
        self.cnt = getulong(item[12:])


class RPMHeader:
    magic = "\x8e\xad\xe8"

    def __init__(self, reader):
        self.__search_header_magic(reader)

        reader.read(8)  # skip: 4 bytes magic, 4 bytes reserved
        self.num_items = readulong(reader)
        self.size_bytes = readulong(reader)
        self.idx_store = reader.read(self.num_items * 16)
        self.store = reader.read(self.size_bytes)
        self.index = [RPMIndexItem(self.idx_store, i) for i in xrange(self.num_items)]

        self.parsers = {6: RPMHeader.__parse_string, 8: RPMHeader.__parse_string_array}

    def getitem(self, tag):
        item = self.__getindex(tag)
        parser = self.parsers.get(item.type)

        if not parser:
            raise Exception("Index entry type %d is not supported" % item.type)

        return parser(self, item)

    def __parse_string(self, item):
        store = self.store[item.off:]
        return store[:store.find('\x00')]

    def __parse_string_array(self, item):
        store = self.store[item.off:]
        rv = []

        for dummy in xrange(item.cnt):
            end = store.find('\x00')
            rv.append(store[:end])
            store = store[end + 1:]

        return rv

    def __getindex(self, tag):
        for i in self.index:
            if i.tag == tag:
                return i
        return None

    def __search_header_magic(self, reader):
        buf = str()

        while not reader.eof:
            buf += reader.read(4)
            v = buf.rfind(RPMHeader.magic)

            if v >= 0:
                buf = buf[v:]
                reader.putback(buf)
                return

        raise Exception("No RPM header")

    def __getstate__(self):
        ret = self.__dict__.copy()
        del(ret['parsers'])
        return ret

    def __setstate__(self, dict):
        self.__dict__ = dict.copy()
        self.parsers = {6: RPMHeader.__parse_string, 8: RPMHeader.__parse_string_array}

# from rpm/rpmlib.h
RPMTAG_NAME = 1000
RPMTAG_VERSION = 1001
RPMTAG_RELEASE = 1002
RPMTAG_ARCH = 1022


class RPMInfo:

    def __init__(self, filename):
        reader = BufferReader(filename)

        try:
            dummy_signature = RPMHeader(reader)
            self.header = RPMHeader(reader)

            g = lambda t: self.header.getitem(t)
            self.name = g(RPMTAG_NAME)
            self.version = g(RPMTAG_VERSION)
            self.release = g(RPMTAG_RELEASE)
            self.arch = g(RPMTAG_ARCH)

        finally:
            reader.close()

    def __str__(self):
        return "%s.rpm" % "-".join([self.name, self.version, self.release, self.arch])

    def __gt__(self, other):
        if LooseVersion(self.version) > LooseVersion(other.version):
            return True
        elif LooseVersion(self.version) == LooseVersion(other.version) and LooseVersion(self.release) > LooseVersion(
                other.release):
            return True
        return False

    def __eq__(self, other):
        if LooseVersion(self.version) == LooseVersion(other.version) and LooseVersion(self.release) == LooseVersion(
                other.release):
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


def fullname(name, version, release):
    return "%s-%s-%s" % (name, version, release)
