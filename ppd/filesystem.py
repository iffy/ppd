
import logging
from stat import S_IFDIR, S_IFLNK, S_IFREG
import errno
from time import time


import sys

from fuse import FUSE, LoggingMixIn, Operations, FuseOSError

if not hasattr(__builtins__, 'bytes'):
    bytes = str


class FileSystem(LoggingMixIn, Operations):

    def __init__(self, ppd):
        self._ppd = ppd

    def _patternFromPath(self, path):
        pattern = {}
        last_key = None
        parts = path.lstrip('/').split('/')
        for segment in parts:
            if not segment:
                continue
            if last_key is None:
                # key
                last_key = segment
                pattern[last_key] = '*'
            else:
                # value
                pattern[last_key] = segment
                last_key = None
        return pattern, last_key

    def chmod(self, path, mode):
        return 0

    def chown(self, path, uid, gid):
        print 'chown', path, uid, gid
        return

    def create(self, path, mode):
        print 'create', path, mode
        return self.fd

    def getattr(self, path, fh=None):
        print '\ngetattr', path, fh
        now = time()
        if path == '/':
            return dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                        st_mtime=now, st_atime=now, st_nlink=2)

        pattern, last_key = self._patternFromPath(path)
        print 'last_key', last_key
        print 'pattern', pattern
        if last_key:
            # key
            return dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                        st_mtime=now, st_atime=now, st_nlink=2)
        else:
            # value
            objects = self._ppd.listObjects(pattern)
            keys = set()
            for obj in objects:
                keys.update([x for x in obj.keys() if not x.startswith('_')])
            print 'objects', objects
            print 'keys', keys
            return dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                        st_mtime=now, st_atime=now, st_nlink=2)
        return dict(st_mode=S_IFREG, st_nlink=1,
                    st_size=0, st_ctime=time(), st_mtime=time(),
                    st_atime=time())
        
        raise FuseOSError(errno.ENOENT)

    def getxattr(self, path, name, position=0):
        print 'getxattr', path, name, position
        return ''

    def listxattr(self, path):
        print 'listxattr', path
        return []

    def mkdir(self, path, mode):
        print 'mkdir', path, mode
        pass

    def open(self, path, flags):
        print 'open', path, flags
        return 0

    def read(self, path, size, offset, fh):
        print 'read', path, size, offset, fh
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        items = set()
        objects = self._ppd.listObjects()
        for obj in objects:
            for k,v in obj.items():
                if not k.startswith('_'):
                    items.add(str(v))

        return ['.', '..'] + sorted(items)

    def readlink(self, path):
        print 'readlink', path
        return self.data[path]

    # def removexattr(self, path, name):
    #     attrs = self.files[path].get('attrs', {})

    #     try:
    #         del attrs[name]
    #     except KeyError:
    #         pass        # Should return ENOATTR

    def rename(self, old, new):
        print 'rename', old, new
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        print 'rmdir', path
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        print 'setxattr', path, name, value, options, position
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        print 'statfs', path
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        print 'symlink', target, source
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        self.data[target] = source

    def truncate(self, path, length, fh=None):
        print 'truncate', path, length, fh
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length

    def unlink(self, path):
        print 'unlink', path
        self.files.pop(path)

    def utimens(self, path, times=None):
        print 'utimens', path, times
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        print 'write', path, data, offset, fh
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)

