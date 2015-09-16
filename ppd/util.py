#!/usr/bin/env python
# Copyright (c) The ppd team
# See LICENSE for details.

from unqlite import UnQLite


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

    def addRecords(self, records):
        self.objects.store(records)
