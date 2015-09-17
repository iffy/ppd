# Copyright (c) The ppd team
# See LICENSE for details.

from unittest import TestCase
from StringIO import StringIO

from ppd.util import PPDInterface


class PPDInterfaceTest(TestCase):


    def test_addFile_getFile(self):
        """
        You can add a file and get the contents back.
        """
        i = PPDInterface()
        fh = StringIO('\x00\x01Hey\xff')
        obj_id = i.addFile(fh, 'something.exe', {})
        obj = i.getObject(obj_id)
        contents = i.getFileContents(obj['_file_id'])
        self.assertEqual(contents, '\x00\x01Hey\xff',
            "Should return the contents provided when attaching the file")
