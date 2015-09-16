#!/usr/bin/env python
# Copyright (c) The ppd team
# See LICENSE for details.

from unqlite import UnQLite

import os
from fnmatch import fnmatch


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
        self.objects.store(objects)


    def listObjects(self, meta_glob=None):
        """
        List objects in the object database.
        """
        if meta_glob is None:
            return self.objects.all()
        else:
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
            return self.objects.filter(filterfunc)



    def addFile(self, fh, filename, metadata):
        """
        Add a file to the store.
        """
        self.files.store({'content': fh.read()})
        metadata = metadata.copy()
        metadata['filename'] = os.path.basename(filename)
        metadata['_file_id'] = self.files.last_record_id()
        self.objects.store(metadata)

