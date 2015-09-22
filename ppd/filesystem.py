
from time import time
from stat import S_IFDIR, S_IFLNK, S_IFREG
from functools import wraps

from StringIO import StringIO

import errno
import yaml
import re
from weakref import WeakKeyDictionary

import subprocess

from fuse import LoggingMixIn, Operations, FuseOSError

if not hasattr(__builtins__, 'bytes'):
    bytes = str


_fd = 0

class BaseResource(object):

    isFile = False
    _now = time()

    def exists(self):
        return True

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
        if self.exists():
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
        raise FuseOSError(errno.ENOENT)

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

    def rename(self, newresource):
        """
        Rename this resource to the given resource.
        """
        raise NotImplemented

    def unlink(self):
        """
        Delete this file.
        """
        raise NotImplemented

    # directory operations

    def mkdir(self, mode):
        raise NotImplemented


class StaticDirectory(BaseResource):

    isFile = False

    def __init__(self, ppd, path):
        self._children = {}
        self.ppd = ppd
        self.path = path

    def purge(self):
        self._children = {}

    def addChild(self, segment, child):
        self._children[segment] = child

    def listChildren(self):
        all_children = self.listStaticChildren() + self.listDynamicChildren()
        return ['.', '..'] + sorted(set(all_children))

    def listStaticChildren(self):
        return sorted(self._children)

    def listDynamicChildren(self):
        objects = self.ppd.listObjects({
            'dirname': self.path,
            '_file_id': '*',
        })
        return [x['filename'] for x in objects]

    def child(self, segment):
        if segment in self._children:
            return self._children[segment]
        
        dynamic = self.listDynamicChildren()
        if segment in dynamic:
            objs = self.ppd.listObjects({
                    'dirname': self.path,
                    '_file_id': '*',
                    'filename': segment,
                })
            return File(self.ppd, objs[0]['_id'], {
                    'dirname': self.path,
                })
        else:
            return PotentialFile(self.ppd,
                {
                    'dirname': self.path,
                    'filename': segment,
                })


_cache = WeakKeyDictionary()
_cache_last_run = WeakKeyDictionary()

class _CacheKey(object):

    def __init__(self, *args):
        self.args = args


def cache(f):
    @wraps(f)
    def deco(self, *args, **kwargs):
        # a string?  really?
        cache_key = getattr(self, '__cache_key__', None)
        if not cache_key:
            self.__cache_key__ = cache_key = _CacheKey(self, f)
        last_updated = self.ppd.last_updated()
        last_run = _cache_last_run.get(cache_key, 0)
        if last_updated > last_run or cache_key not in _cache:
            _cache[cache_key] = f(self, *args, **kwargs)
            _cache_last_run[cache_key] = self.ppd.last_updated()
        return _cache[cache_key]
    return deco



class ScriptableFile(BaseResource):

    isFile = True
    _last_run = 0

    def __init__(self, ppd, in_script=None, out_script=None):
        self.ppd = ppd
        self.in_script = in_script
        self.out_script = out_script

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

    def exists(self):
        return len(self.ppd.listObjects(self.filter))

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
            return File(self.ppd, files[0]['_id'], pattern)
        else:
            return PotentialFile(self.ppd, pattern)

    def mkdir(self, mode):
        pattern = {}
        for k,v in self.filter.items():
            if v == '*':
                continue
            pattern[k] = v
        self.ppd.addObject(pattern)


class File(BaseResource):

    isFile = True

    def __init__(self, ppd, object_id, pattern):
        self.ppd = ppd
        self.object_id = object_id
        self.pattern = pattern

    def get_size(self):
        content = self.ppd.getFileContents(self.object_id)
        return len(content)

    def read(self, size, offset):
        return self.ppd.getFileContents(self.object_id)[offset:offset+size]

    def rename(self, newresource):
        if isinstance(newresource, PotentialFile):
            # it's another file, just change the name.
            print 'renaming ->', newresource.metadata['filename']
            self.ppd.updateObjects({'filename': newresource.metadata['filename']},
                                   {'_id': str(self.object_id)})
        elif isinstance(newresource, (File, ConfigFile)):
            # write the file.
            # This is probably not a good way to do this.
            newresource.write(self.read(self.get_size(), 0), 0)
            self.unlink()
        else:
            raise NotImplemented

    def truncate(self, length):
        print self, 'truncate', length
        self.ppd.setFileContents(self.object_id, StringIO(' ' * length))

    def unlink(self):
        obj = self.ppd.getObject(self.object_id)
        new_obj = obj.copy()
        for k in self.pattern:
            if k == 'filename':
                continue
            new_obj.pop(k)
        leftovers = [x for x in new_obj if not x.startswith('_')]
        leftovers.remove('filename')
        if leftovers:
            # this file still has other associated metadata.  Don't delete it
            self.ppd.replaceObject(self.object_id, new_obj)
        else:
            self.ppd.deleteObject(self.object_id)

    def write(self, data, offset):
        print self, 'write', repr(data), offset
        content = StringIO(self.ppd.getFileContents(self.object_id))
        content.seek(offset)
        content.write(data)
        content.seek(0)
        self.ppd.setFileContents(self.object_id, content)
        return len(data)


class ConfigFile(BaseResource):

    isFile = True

    def __init__(self, ppd, layout, root):
        self.ppd = ppd
        self.root = root
        if layout:
            self.layout = ppd.setCurrentFSLayout(layout)
        else:
            self.layout = ppd.getCurrentFSLayout()

    @cache
    def getContent(self):
        return yaml.safe_dump(self.ppd.getCurrentFSLayout())

    def get_size(self):
        return len(self.getContent())

    def read(self, size, offset):
        content = self.getContent()
        print 'reading config', content
        return content[offset:offset+size]

    def write(self, data, offset):
        print 'writing', repr(data), offset
        content = StringIO(self.getContent())
        content.seek(offset)
        content.write(data)
        content.truncate()
        content.seek(0)
        print 'content', repr(content.getvalue())
        try:
            data = yaml.safe_load(content)
        except Exception:
            print 'bad config'
            return 0
        print 'data', data
        self.ppd.setCurrentFSLayout(data)
        print 'set current fs layout'
        configureRoot(self.ppd, self.root, data)
        print 'reconfigured root'

    def unlink(self):
        raise NotImplemented


class PotentialFile(BaseResource):

    isFile = True

    def __init__(self, ppd, metadata):
        self.ppd = ppd
        self.metadata = metadata

    def exists(self):
        return False

    def create(self, mode):
        print self, 'create', mode, self.metadata
        self.ppd.addFile(StringIO(''), None, self.metadata)
        return 0


file_types = {
    'scriptable': ScriptableFile,
    'objdir': ObjectDirectory,
}


def configureRoot(ppd, root, layout):
    root.purge()
    try:
        if not layout:
            layout = ppd.getCurrentFSLayout()
        paths = layout.get('paths', [])
        for item in paths:
            item = item.copy()
            segment = item.pop('path')
            item_type = item.keys()[0]
            kwargs = item[item_type]
            cls = file_types[item_type]
            root.addChild(segment, cls(ppd, **kwargs))
    except Exception as e:
        print 'Error', e

    # config file
    root.addChild('config.yml', ConfigFile(ppd, layout, root))
    return root


def generateRoot(ppd, layout):
    root = StaticDirectory(ppd, '/')
    configureRoot(ppd, root, layout)
    return root


def getFileSystem(ppd, layout):
    return FileSystem(generateRoot(ppd, layout))


class FileSystem(LoggingMixIn, Operations):

    def __init__(self, resource):
        self.root = resource

    def resource(self, path):
        path = path.lstrip('/')
        try:
            ret = self.root.childFromPath(path)
            print '  -- ', ret
            return ret
        except KeyError:
            raise FuseOSError(errno.ENOENT)

    # ---------------------------

    def access(self, path, amode):
        #print 'access', path, amode
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
        return self.resource(old).rename(self.resource(new))

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
        self.resource(path).unlink()

    def utimens(self, path, times=None):
        print 'utimens', path, times
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        print 'write', path, data, offset, fh
        return self.resource(path).write(data, offset)

