# Copyright (c) The ppd team
# See LICENSE for details.

try:
    from pysqlite2 import dbapi2 as sqlite
except ImportError:
    import sqlite3 as sqlite

import os
import yaml
import time
import json
from fnmatch import fnmatch
from functools import partial, wraps
from hashlib import sha1

from StringIO import StringIO

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

def autocommit(f):
    @wraps(f)
    def deco(self, *args, **kwargs):
        self.db.commit()
        ret = f(self, *args, **kwargs)
        self.db.commit()
        return ret
    return deco


# This mimicks a key-value store and doesn't use some of SQL's
# best features.  This is (sort of) intentional.
CREATE_SQL = [
    '''CREATE TABLE IF NOT EXISTS keyvalues (
        key blob,
        value blob,
        UNIQUE (key)
    )''',
    '''CREATE TABLE IF NOT EXISTS objects (
        _id INTEGER PRIMARY KEY,
        _created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        collection_key TEXT,
        data BLOB
    )''',
    '''CREATE TABLE IF NOT EXISTS files (
        _id INTEGER PRIMARY KEY,
        _created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        content BLOB
    )''',
]


class Collection(object):

    def __init__(self, db, name):
        self.db = db
        self.name = name

    def _combine(self, x):
        data = json.loads(x[1])
        data.update({
            '_id': x[0],
        })
        return data

    def add(self, data):
        encoded = json.dumps(data)
        r = self.db.execute('insert into objects (collection_key, data)'
            ' values (?, ?)', (self.name, encoded))
        ident = r.lastrowid
        return ident

    def list(self, filter_fn=None):
        r = self.db.execute('select _id, data from objects'
            ' where collection_key=?', (self.name,))
        return filter(filter_fn, map(self._combine, r.fetchall()))

    def fetch(self, id):
        r = self.db.execute('select _id, data from objects'
            ' where collection_key=? and _id=?', (self.name, id))
        row = r.fetchone()
        if not row:
            return {}
        return self._combine(row)

    def update(self, id, data):
        encoded = json.dumps(data)
        self.db.execute('update objects set data=?'
            ' where collection_key=? and _id=?', (encoded, self.name, id))

    def delete(self, id):
        self.db.execute('delete from objects'
            ' where collection_key=? and _id=?', (self.name, id))


class KeyValue(object):

    def __init__(self, db):
        self.db = db

    @autocommit
    def __getitem__(self, key):
        r = self.db.execute('select value from keyvalues where key=?', (key,))
        row = r.fetchone()
        if not row:
            raise KeyError(key)
        return str(row[0])

    @autocommit
    def __setitem__(self, key, value):
        try:
            self.db.execute('insert into keyvalues (key, value) values (?,?)',
                (key, buffer(value)))
        except sqlite.IntegrityError:
            self.db.execute('update keyvalues set value=? where key=?',
                (buffer(value), key))

    @autocommit
    def get(self, key, default):
        try:
            return self[key]
        except KeyError:
            return default


class FileStore(object):

    def __init__(self, db):
        self.db = db

    @autocommit
    def add(self, fh):
        data = fh.read()
        h = hashFile(StringIO(data))
        r = self.db.execute('insert into files (content) values (?)', (buffer(data),))
        ident = r.lastrowid
        return ident, h

    @autocommit
    def getContent(self, file_id):
        r = self.db.execute('select content from files where _id=?', (file_id,))
        row = r.fetchone()
        if not row:
            raise KeyError(file_id)
        return str(row[0])

    @autocommit
    def setContent(self, file_id, fh):
        self.db.execute('update files set content=? where _id=?',
            (buffer(fh.read()), file_id))

    @autocommit
    def delete(self, file_id):
        self.db.execute('delete from files where _id=?', (file_id,))



class PPD(object):

    def __init__(self, dbfile=':memory:', dumper=None, auto_dump=False):
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

    def markUpdated(f):
        @wraps(f)
        def deco(self, *args, **kwargs):
            ret = f(self, *args, **kwargs)
            self.kv['sys:last_updated'] = str(float(time.time()))
            return ret
        return deco
    
    _db = None
    @property
    def db(self):
        if self._db is None:
            self._db = sqlite.connect(self.dbfile, check_same_thread=False)
            for sql in CREATE_SQL:
                self._db.execute(sql)
        return self._db

    @property
    def kv(self):
        return KeyValue(self.db)

    _objects = None
    @property
    def objects(self):
        if self._objects is None:
            self._objects = Collection(self.db, 'objects')
        return self._objects


    def commit(self):
        self.db.commit()


    def close(self):
        self.db.close()
        self._objects = None
        self._db = None


    @autocommit
    def last_updated(self):
        """
        Get the timestamp when the database was last updated
        """
        try:
            return float(self.kv['sys:last_updated'])
        except KeyError:
            return 0.0

    @autocommit
    def dump(self, filter_glob=None):
        for obj in self.listObjects(filter_glob=filter_glob):
            self.dumper.dumpObject(obj)


    @autocommit
    @markUpdated
    @autoDump
    def addObject(self, obj):
        """
        Add an object to the obj database.
        """
        return self.objects.add(obj)

    @autocommit
    def getObject(self, object_id):
        """
        Get an object by its id
        """
        return self.objects.fetch(object_id)

    @autocommit
    @markUpdated
    def deleteObject(self, object_id):
        """
        Delete an object by id.
        """
        obj = self.getObject(object_id)
        if '_file_id' in obj:
            FileStore(self.db).delete(obj['_file_id'])
        self.objects.delete(object_id)

    @autocommit
    @markUpdated
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
                self.objects.update(new_obj['_id'], new_obj)
                logger.msg('updated', obj_id=new_obj['_id'])
            ret.append(new_obj)
        return ret


    @autocommit
    @markUpdated
    @autoDump
    def replaceObject(self, object_id, data):
        """
        Replace an object with the given data.
        """
        self.objects.update(object_id, data)

    @autocommit
    def listObjects(self, filter_glob=None, id_only=False):
        """
        List objects in the object database.
        """
        filter_fn = None
        if filter_glob is not None:
            filter_fn = mkFilterFunc(filter_glob)
        func = partial(self.objects.list, filter_fn)

        if id_only:
            return [x['_id'] for x in func()]
        else:
            return func()

    @autocommit
    @markUpdated
    @autoDump
    def addFile(self, fh, filename, metadata):
        """
        Add a file to the store.
        """
        file_id, file_hash = FileStore(self.db).add(fh)
        metadata = metadata.copy()
        filename = filename or metadata.get('filename', None)
        if not filename:
            raise ValueError('No filename provided')
        metadata['filename'] = os.path.basename(filename)
        metadata['_file_id'] = file_id
        metadata['_file_hash'] = file_hash
        return self.objects.add(metadata)

    @autocommit
    def getFileContents(self, obj_id):
        """
        Get the file contents.
        """
        obj = self.getObject(obj_id)
        return FileStore(self.db).getContent(obj['_file_id'])

    @autocommit
    def setFileContents(self, obj_id, fh):
        obj = self.getObject(obj_id)
        content = fh.read()
        file_hash = hashFile(StringIO(content))
        obj['_file_hash'] = file_hash
        self.objects.update(obj['_id'], obj)
        FileStore(self.db).setContent(obj['_file_id'], StringIO(content))

    #-------------------------------
    # filesystem stuff
    #-------------------------------

    @autocommit
    def getCurrentFSLayout(self):
        s = self.kv.get('sys:current_layout', '{}')
        return yaml.safe_load(s)

    @autocommit
    def setCurrentFSLayout(self, layout):
        self.kv['sys:current_layout'] = yaml.safe_dump(layout)
        return layout


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

