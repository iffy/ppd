# Copyright (c) The ppd team
# See LICENSE for details.

from unqlite import UnQLite

import os
import yaml
from uuid import uuid4
from fnmatch import fnmatch
from functools import partial, wraps
from hashlib import sha1

from structlog import get_logger
logger = get_logger()

def mkFilterFunc(filter_glob):
    def filterfunc(obj):
        for k,pattern in filter_glob.items():
            if k not in obj:
                return False
            value = obj[k]
            if not isinstance(value, (unicode, str)):
                value = str(value)
            if not fnmatch(value, pattern):
                return False
        return True
    return filterfunc


def hashFile(fh, chunk_size=1024):
    original_seek = fh.tell()
    fh.seek(0)
    h = sha1()
    while True:
        chunk = fh.read(chunk_size)
        if not chunk:
            break
        h.update(chunk)
    fh.seek(original_seek)
    return h.hexdigest()


class PPD(object):

    def __init__(self, dbfile=':mem:', dumper=None, auto_dump=False):
        """
        If no dbfile is provided, an in-memory database will be used.
        """
        self.dbfile = dbfile
        self.dumper = dumper
        self.auto_dump = auto_dump
        if dumper:
            dumper.ppd = self

    def autoDump(f):
        @wraps(f)
        def deco(self, *args, **kwargs):
            ret = f(self, *args, **kwargs)
            if self.auto_dump:
                objects = ret
                if not isinstance(ret, (tuple, list)):
                    objects = [ret]
                for obj in objects:
                    if isinstance(obj, int):
                        # it's an object id
                        obj = self.objects.fetch(obj)
                    self.dumper.dumpObject(obj)
            return ret
        return deco
    
    _db = None
    @property
    def db(self):
        if self._db is None:
            self._db = UnQLite(self.dbfile)
        return self._db


    _objects = None
    @property
    def objects(self):
        if self._objects is None:
            self._objects = self.db.collection('objects')
            if not self._objects.exists():
                self._objects.create()
        return self._objects


    def commit(self):
        self.db.commit()


    def close(self):
        self.db.close()


    def dump(self, filter_glob=None):
        for obj in self.listObjects(filter_glob=filter_glob):
            self.dumper.dumpObject(obj)


    @autoDump
    def addObject(self, obj):
        """
        Add an object to the obj database.
        """
        return self.objects.store(obj)

    def getObject(self, object_id):
        """
        Get an object by its id
        """
        return self.objects.fetch(object_id)

    def deleteObject(self, object_id):
        """
        Delete an object by id.
        """
        obj = self.getObject(object_id)
        if '_file_id' in obj:
            self.db.delete(obj['_file_id'])
        self.objects.delete(object_id)

    @autoDump
    def updateObjects(self, data, filter_glob=None):
        """
        For each matching object, merge in the given data.
        """
        ret = []
        for obj in self.listObjects(filter_glob):
            new_obj = obj.copy()
            new_obj.update(data)
            if new_obj != obj:
                self.objects.update(new_obj['__id'], new_obj)
                logger.msg('updated', obj_id=new_obj['__id'])
            ret.append(new_obj)
        return ret


    def listObjects(self, filter_glob=None, id_only=False):
        """
        List objects in the object database.
        """
        func = None
        if filter_glob is None:
            func = partial(self.objects.all)
        else:
            func = partial(self.objects.filter, mkFilterFunc(filter_glob))

        if id_only:
            return [x['__id'] for x in func()]
        else:
            return func()

    @autoDump
    def addFile(self, fh, filename, metadata):
        """
        Add a file to the store.
        """
        file_id = 'file-{0}'.format(uuid4())
        self.db[file_id] = fh.read()
        metadata = metadata.copy()
        filename = filename or metadata.get('filename', None)
        if not filename:
            raise ValueError('No filename provided')
        metadata['filename'] = os.path.basename(filename)
        metadata['_file_id'] = file_id
        metadata['_file_hash'] = hashFile(fh)
        return self.objects.store(metadata)


    def getFileContents(self, file_id):
        """
        Get the file contents.
        """
        return self.db[file_id]



class RuleBasedFileDumper(object):

    
    def __init__(self, root, rules=None, ppd=None, reporter=None):
        self.root = root
        self.ppd = ppd
        self._rules = rules
        self.reporter = reporter or (lambda x:None)


    _compiled_rules = None
    @property
    def rules(self):
        if self._compiled_rules is None:
            self._compiled_rules = []
            for rule in self._rules:
                if rule['pattern'] == 'all':
                    rule['$match_fn'] = lambda x:True
                else:
                    rule['$match_fn'] = mkFilterFunc(rule['pattern'])
                self._compiled_rules.append(rule)
        return self._compiled_rules


    def dumpObject(self, obj):
        """
        Dump this object according to the rules.
        """
        for rule in self.rules:
            if rule['$match_fn'](obj):
                for action in rule['actions']:
                    self.performAction(action, obj)


    def performAction(self, action, obj):
        """
        Perform a single action on a single object.
        """
        if 'merge_yaml' in action:
            self._perform_merge_yaml(action, obj)
        if 'write_file' in action:
            self._perform_write_file(action, obj)


    def _perform_merge_yaml(self, action, obj):
        """
        Perform merge_yaml
        """
        filename = action['merge_yaml'].format(**obj)
        fullpath = os.path.join(self.root, filename)
        dirname = os.path.dirname(fullpath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        current_val = None
        if os.path.exists(fullpath):
            with open(fullpath, 'rb') as fh:
                current_val = yaml.safe_load(fh)

        new_val = {}
        if current_val:
            new_val = current_val.copy()
        new_val.update(obj)

        if new_val != current_val:
            with open(fullpath, 'wb') as fh:
                fh.write(yaml.safe_dump(new_val, default_flow_style=False))
                self.reporter('wrote {0}'.format(fullpath))


    def _perform_write_file(self, action, obj):
        """
        Write a file's contents to disk.
        """
        filename = action['write_file'].format(**obj)
        fullpath = os.path.join(self.root, filename)
        dirname = os.path.dirname(fullpath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        existing_hash = 'not a real hash sentinal'
        if os.path.exists(fullpath):
            with open(fullpath, 'rb') as fh:
                existing_hash = hashFile(fh)

        if existing_hash != obj['_file_hash']:
            with open(fullpath, 'wb') as fh:
                fh.write(self.ppd.getFileContents(obj['_file_id']))
                self.reporter('wrote {0}'.format(fullpath))

