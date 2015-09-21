
from time import time
from stat import S_IFDIR, S_IFLNK, S_IFREG
from functools import wraps

from StringIO import StringIO

import errno
import yaml
import re

import subprocess

from fuse import LoggingMixIn, Operations, FuseOSError

if not hasattr(__builtins__, 'bytes'):
    bytes = str


_fd = 0

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
            return child.childFromPath(parts[1])
        else:
            return child

    # file operations

    def open(self, flags):
        global _fd
        _fd += 1
        return _fd
        
    def read(self, size, offset):
        return ''

    def write(self, data, offset):
        return len(data)

    def create(self, mode):
        raise NotImplemented


    # directory operations

    def mkdir(self, mode):
        raise NotImplemented


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
    _last_run = 0

    def __init__(self, ppd, in_script=None, out_script=None):
        self.ppd = ppd
        self.in_script = in_script
        self.out_script = out_script

    _cache = {}
    def cache(f):
        cache_key = f.__name__
        @wraps(f)
        def deco(self, *args, **kwargs):
            last_updated = self.ppd.last_updated()
            print 'last_updated', last_updated
            print 'last_run    ', self._last_run
            if last_updated > self._last_run or cache_key not in self._cache:
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

    def get_mtime(self):
        return int(self._last_run)

    def open(self, flags):
        return 0

    def read(self, size, offset):
        return self._runOutputScript()[offset:offset+size]


class ObjectDirectory(BaseResource):

    isFile = False
    r_display = re.compile(r'({.*?})')

    def __init__(self, ppd, display):
        self.ppd = ppd
        data = self.processDisplay(display)
        self.regex = data['regex']
        self.pattern = data['pattern']
        self.display = data['display']


    def processDisplay(self, display):
        parts = self.r_display.split(display)
        keys = []
        regex = []
        for part in parts:
            if part.startswith('{'):
                key = part[1:-1]
                keys.append(key)
                regex.append(r'(?P<' + key + '>.*?)')
            else:
                regex.append(part)
        pattern = {}
        for key in keys:
            pattern[key] = '*'
        
        return {
            'display': display,
            'pattern': pattern,
            'regex': re.compile('^' + ''.join(regex) + '$'),
        }

    def listChildren(self):
        objects = self.ppd.listObjects(self.pattern)
        displays = [self.display.format(**x) for x in objects]
        unique = set(displays)
        return ['.', '..'] + sorted(unique)

    def child(self, segment):
        m = self.regex.match(segment)
        print 'match', m
        print m.groupdict()
        pattern = self.pattern.copy()
        for k,v in m.groupdict().items():
            pattern[k] = v
        return SingleObjectDirectory(self.ppd, pattern)

    def mkdir(self, mode):
        print self, 'mkdir', mode


class SingleObjectDirectory(BaseResource):

    isFile = False

    def __init__(self, ppd, filter):
        self.ppd = ppd
        self.filter = filter

    def listChildren(self):
        pattern = self.filter.copy()
        pattern['_file_id'] = '*'
        files = self.ppd.listObjects(pattern)
        names = [x['filename'] for x in files]
        return ['.', '..'] + sorted(names)

    def child(self, segment):
        pattern = self.filter.copy()
        pattern['filename'] = segment
        files = self.ppd.listObjects(pattern)
        if files:
            return File(self.ppd, files[0]['_id'])
        else:
            return PotentialFile(self.ppd, pattern)

    def mkdir(self, mode):
        print self, 'mkdir', mode


class File(BaseResource):

    isFile = True

    def __init__(self, ppd, object_id):
        self.ppd = ppd
        self.object_id = object_id

    def get_size(self):
        content = self.ppd.getFileContents(self.object_id)
        return len(content)

    def read(self, size, offset):
        return self.ppd.getFileContents(self.object_id)[offset:offset+size]

    def truncate(self, length):
        print self, 'truncate', length
        self.ppd.setFileContents(self.object_id, StringIO(''))

    def write(self, data, offset):
        print self, 'write', repr(data), offset
        content = StringIO(self.ppd.getFileContents(self.object_id))
        content.seek(offset)
        content.write(data)
        self.ppd.setFileContents(self.object_id, content)
        return len(data)



class PotentialFile(BaseResource):

    isFile = True

    def __init__(self, ppd, metadata):
        self.ppd = ppd
        self.metadata = metadata

    def create(self, mode):
        print self, 'create', mode
        self.ppd.addFile(StringIO(''), None, self.metadata)
        return 0


file_types = {
    'scriptable': ScriptableFile,
    'objdir': ObjectDirectory,
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

    # ---------------------------

    def access(self, path, amode):
        print 'access', path, amode
        return 0

    def bmap(self, path, blocksize, idx):
        print 'bmap', path, blocksize, idx

    def chmod(self, path, mode):
        return 0

    def chown(self, path, uid, gid):
        print 'chown', path, uid, gid
        return

    def create(self, path, mode):
        print 'create', path, mode
        return self.resource(path).create(mode)

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
        return self.resource(path).mkdir(mode)

    def open(self, path, flags):
        print 'open', path, flags
        return self.resource(path).open(flags)

    def read(self, path, size, offset, fh):
        print 'read', path, size, offset, fh
        return self.resource(path).read(size, offset)

    def fsync(self, path, datasync, fh):
        print 'fsync', path, datasync, fh

    def fsyncdir(self, *args, **kwargs):
        print 'fsyncdir', args, kwargs

    def flush(self, path, fh):
        print 'flush', path, fh

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

    def release(self, path, fip):
        print 'release', path, fip

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
        return self.resource(path).truncate(length)

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
        return self.resource(path).write(data, offset)

