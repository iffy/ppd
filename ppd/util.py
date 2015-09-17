# Copyright (c) The ppd team
# See LICENSE for details.

from unqlite import UnQLite

import os
import yaml
from uuid import uuid4
from fnmatch import fnmatch
from functools import partial

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


class PPD(object):

    def __init__(self, dbfile=':mem:'):
        """
        If no dbfile is provided, an in-memory database will be used.
        """
        self.dbfile = dbfile
    
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

    def updateObjects(self, data, filter_glob=None):
        """
        For each matching object, merge in the given data.
        """
        ret = []
        for obj in self.listObjects(filter_glob):
            new_obj = obj.copy()
            new_obj.update(data)
            if new_obj != obj:
                logger.msg('Updating', obj=new_obj)
                self.objects.update(new_obj['__id'], new_obj)
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


    def addFile(self, fh, filename, metadata):
        """
        Add a file to the store.
        """
        file_id = 'file-{0}'.format(uuid4())
        self.db[file_id] = fh.read()
        metadata = metadata.copy()
        metadata['filename'] = os.path.basename(filename)
        metadata['_file_id'] = file_id
        return self.objects.store(metadata)


    def getFileContents(self, file_id):
        """
        Get the file contents.
        """
        return self.db[file_id]


    def _compileLayout(self, layout):
        if 'compiled' not in layout:
            for rule in layout['rules']:
                rule['fn'] = mkFilterFunc(rule['pattern'])
                if not isinstance(rule['dst'], (list, tuple)):
                    rule['dst'] = [rule['dst']]
            layout['compiled'] = True
        return layout

    def dumpObjectToFiles(self, basedir, layout, obj):
        self._compileLayout(layout)
        file_id = obj.get('_file_id', None)
        for rule in layout['rules']:
            if rule['fn'](obj):
                for dst in rule['dst']:
                    formatted_dst = dst['path'].format(**obj)
                    if file_id is not None:
                        self._writeRawFile(basedir, formatted_dst, obj)
                    else:
                        self._mergeObjectToFile(basedir, formatted_dst, obj)
                break

    def _writeRawFile(self, basedir, dst, obj):
        logger.msg('_writeRawFile')
        fullpath = os.path.join(basedir, dst)
        dirname = os.path.dirname(fullpath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        with open(fullpath, 'wb') as fh:
            logger.msg('Writing file', fullpath=fullpath)
            fh.write(self.getFileContents(obj['_file_id']))

    def _mergeObjectToFile(self, basedir, dst, obj):
        """
        Merge a YAML object with existing file.
        """
        fullpath = os.path.join(basedir, dst)
        dirname = os.path.dirname(fullpath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        existing = {}
        towrite = obj
        if os.path.exists(fullpath):
            existing = yaml.safe_load(open(fullpath, 'rb'))
            towrite = existing
            towrite.update(obj)
        if existing != towrite:
            with open(fullpath, 'wb') as fh:
                logger.msg('Writing', filename=fullpath)
                fh.write(yaml.safe_dump(existing,
                    default_flow_style=False))


class RuleBasedFileDumper(object):

    pass



