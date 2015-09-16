#!/usr/bin/env python
# Copyright (c) The ppd team
# See LICENSE for details.

from unqlite import UnQLite

import os
import yaml
from fnmatch import fnmatch

from structlog import get_logger
logger = get_logger()

def mkFilterFunc(meta_glob):
    def filterfunc(obj):
        for k,pattern in meta_glob.items():
            if k not in obj:
                return False
            value = obj[k]
            if not isinstance(value, (unicode, str)):
                value = str(value)
            if not fnmatch(value, pattern):
                return False
        return True
    return filterfunc


class PPDInterface(object):

    def __init__(self, dbfile):
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

    _files = None
    @property
    def files(self):
        if self._files is None:
            self._files = self.db.collection('files')
            if not self._files.exists():
                self._files.create()
        return self._files

    def addObjects(self, objects):
        """
        Add objects to the object database.
        """
        return self.objects.store(objects)


    def listObjects(self, meta_glob=None):
        """
        List objects in the object database.
        """
        if meta_glob is None:
            return self.objects.all()
        else:
            return self.objects.filter(mkFilterFunc(meta_glob))


    def addFile(self, fh, filename, metadata):
        """
        Add a file to the store.
        """
        self.files.store({'content': fh.read()})
        metadata = metadata.copy()
        metadata['filename'] = os.path.basename(filename)
        metadata['_file_id'] = self.files.last_record_id()
        return self.objects.store(metadata)


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

        guts = self.files.fetch(obj['_file_id'])['content']
        with open(fullpath, 'wb') as fh:
            logger.msg('Writing file', fullpath=fullpath)
            fh.write(guts)

    def _mergeObjectToFile(self, basedir, dst, obj):
        """

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




