
from time import time
from stat import S_IFDIR, S_IFLNK, S_IFREG
from functools import wraps

import errno
import yaml

import subprocess

from fuse import LoggingMixIn, Operations, FuseOSError

if not hasattr(__builtins__, 'bytes'):
    bytes = str



class BaseResource(object):

    isFile = False
    _now = time()

    def listChildren(self):
        raise FuseOSError(errno.ENOENT)

    def get_size(self):
        return 0

    def get_ctime(self):
        return self._now

    def get_mtime(self):
        return self._now

    def get_atime(self):
        return self._now

    def getattr(self):
        if self.isFile:
            return dict(st_mode=S_IFREG, st_nlink=1,
                    st_size=self.get_size(),
                    st_ctime=self.get_ctime(),
                    st_mtime=self.get_mtime(),
                    st_atime=self.get_atime()) 
        else:
            return dict(st_mode=(S_IFDIR | 0755),
                        st_ctime=self.get_ctime(),
                        st_mtime=self.get_mtime(),
                        st_atime=self.get_atime(),
                        st_nlink=2)

    def child(self, segment):
        raise NotImplemented

    def childFromPath(self, path):
        if not path:
            return self
        parts = path.split('/', 1)
        child = self.child(parts[0])
        if len(parts) > 1:
            return child.getChild(parts[1])
        else:
            return child

    # file operations

    def open(self, flags):
        return 0
        
    def read(self, size, offset):
        return ''


class StaticDirectory(BaseResource):

    isFile = False

    def __init__(self):
        self._children = {}

    def addChild(self, segment, child):
        self._children[segment] = child

    def listChildren(self):
        print 'StaticDirectory.listChildren'
        return ['.', '..'] + sorted(self._children)

    def child(self, segment):
        return self._children[segment]




class ScriptableFile(BaseResource):

    isFile = True
    _last_run = -1

    def __init__(self, ppd, in_script=None, out_script=None):
        self.ppd = ppd
        self.in_script = in_script
        self.out_script = out_script

    _cache = {}
    def cache(f):
        cache_key = f.__name__
        @wraps(f)
        def deco(self, *args, **kwargs):
            if self.ppd.last_updated() > self._last_run:
                self._cache[cache_key] = f(self, *args, **kwargs)
                self._last_run = self.ppd.last_updated()
            return self._cache[cache_key]
        return deco

    @cache
    def _runOutputScript(self):
        print '_runOutputScript'
        objects = self.ppd.listObjects()
        yaml_string = yaml.safe_dump(objects, default_flow_style=False)
        p = subprocess.Popen(self.out_script,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        out, err = p.communicate(yaml_string)
        return out

    def get_size(self):
        return len(self._runOutputScript())

    def open(self, flags):
        return 0

    def read(self, size, offset):
        return self._runOutputScript()[offset:offset+size]


file_types = {
    'scriptable': ScriptableFile,
}


def generateRoot(ppd, paths):
    root = StaticDirectory()
    for item in paths:
        segment = item.pop('path')
        item_type = item.keys()[0]
        kwargs = item[item_type]
        cls = file_types[item_type]
        root.addChild(segment, cls(ppd, **kwargs))
    return root


def getFileSystem(ppd, paths):
    return FileSystem(generateRoot(ppd, paths))


class FileSystem(LoggingMixIn, Operations):

    def __init__(self, resource):
        self.root = resource

    def resource(self, path):
        path = path.lstrip('/')
        try:
            ret = self.root.childFromPath(path)
            print ret
            return ret
        except KeyError:
            raise FuseOSError(errno.ENOENT)

    def chmod(self, path, mode):
        return 0

    def chown(self, path, uid, gid):
        print 'chown', path, uid, gid
        return

    def create(self, path, mode):
        print 'create', path, mode
        return self.fd

    def getattr(self, path, fh=None):
        print 'getattr', path, fh
        return self.resource(path).getattr()

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
        return self.resource(path).open(flags)

    def read(self, path, size, offset, fh):
        print 'read', path, size, offset, fh
        return self.resource(path).read(size, offset)

    def readdir(self, path, fh):
        print 'readdir', path, fh
        return self.resource(path).listChildren()

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

