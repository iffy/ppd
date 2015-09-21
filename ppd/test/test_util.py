# Copyright (c) The ppd team
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.python.filepath import FilePath
from StringIO import StringIO

import yaml
from ppd.util import PPD, RuleBasedFileDumper


class PPDTest(TestCase):

    def test_addObject(self):
        """
        You can add an object.
        """
        i = PPD()
        object_id = i.addObject({'foo': 'bar'})
        obj = i.getObject(object_id)
        self.assertEqual(obj['foo'], 'bar')
        self.assertEqual(obj['_id'], object_id)


    def test_listObjects_none(self):
        """
        If there are no objects, return an empty list, not None
        """
        i = PPD()
        self.assertEqual(i.listObjects(), [])


    def test_listObjects_all(self):
        """
        You can list all objects.
        """
        i = PPD()
        id1 = i.addObject({'foo': 'bar'})
        id2 = i.addObject({'hey': 'ho'})
        objects = i.listObjects()
        obj1 = i.getObject(id1)
        obj2 = i.getObject(id2)
        self.assertEqual(objects, [obj1, obj2],
            "Should return both objects")


    def test_listObjects_globFilter_value(self):
        """
        You can filter objects by glob pattern.
        """
        i = PPD()
        id1 = i.addObject({'foo': 'bar'})
        i.addObject({'hey': 'ho'})
        obj1 = i.getObject(id1)

        objects = i.listObjects({'foo': 'bar'})
        self.assertEqual(objects, [obj1])

        objects = i.listObjects({'foo': 'b*'})
        self.assertEqual(objects, [obj1])


    def test_listObjects_id(self):
        """
        You can just list object ids.
        """
        i = PPD()
        id1 = i.addObject({'foo': 'bar'})
        id2 = i.addObject({'hey': 'ho'})

        objects = i.listObjects(id_only=True)
        self.assertEqual(objects, [id1, id2])

        objects = i.listObjects({'foo': 'bar'}, id_only=True)
        self.assertEqual(objects, [id1])

        objects = i.listObjects({'foo': 'b*'}, id_only=True)
        self.assertEqual(objects, [id1])


    def test_updateObjects(self):
        """
        You can update all objects.
        """
        i = PPD()
        id1 = i.addObject({'foo': 'bar'})
        id2 = i.addObject({'hey': 'ho'})

        objects = i.updateObjects({'A': 'A'})
        self.assertEqual(len(objects), 2, "Should return matched objects")
        self.assertEqual(objects[0], {
            '_id': id1,
            'foo': 'bar',
            'A': 'A',
        })
        self.assertEqual(objects[1], {
            '_id': id2,
            'hey': 'ho',
            'A': 'A',
        })


    def test_updateObjects_filter(self):
        """
        You can update some objects.
        """
        i = PPD()
        id1 = i.addObject({'foo': 'bar'})
        id2 = i.addObject({'hey': 'ho'})

        objects = i.updateObjects({'A': 'A'}, {'foo': '*'})
        self.assertEqual(len(objects), 1, "Should return matched objects")
        self.assertEqual(objects[0], {
            '_id': id1,
            'foo': 'bar',
            'A': 'A',
        })

        objects = i.listObjects()
        self.assertEqual(objects[1], {
            '_id': id2,
            'hey': 'ho',
        }, "Should have left other object alone")


    def test_updateObjects_noChange(self):
        """
        Matched objects should be returned whether they were updated or not.
        """
        i = PPD()
        id1 = i.addObject({'foo': 'bar'})
        i.addObject({'hey': 'ho'})

        objects = i.updateObjects({'foo': 'bar'}, {'foo': '*'})
        self.assertEqual(len(objects), 1,
            "Should return matching objects")
        self.assertEqual(objects[0], {
            '_id': id1,
            'foo': 'bar',
        })


    def test_deleteObject(self):
        """
        You can delete objects by id
        """
        i = PPD()
        id1 = i.addObject({'foo': 'bar'})
        i.deleteObject(id1)

        objects = i.listObjects()
        self.assertEqual(len(objects), 0,
            "Should have deleted the object")


    def test_addFile_getFile(self):
        """
        You can add a file and get the contents back.
        """
        i = PPD()
        fh = StringIO('\x00\x01Hey\xff')
        obj_id = i.addFile(fh, 'something.exe', {'hey': 'ho'})
        obj = i.getObject(obj_id)
        self.assertEqual(obj['filename'], 'something.exe')
        self.assertEqual(obj['hey'], 'ho')
        self.assertIn('_file_hash', obj, "Should include hash of file")
        contents = i.getFileContents(obj['_file_id'])
        self.assertEqual(contents, '\x00\x01Hey\xff',
            "Should return the contents provided when attaching the file"
            " not: %r" % (contents,))


    def test_addFile_filenameFromMetadata(self):
        """
        You can provide the filename in the metadata to override the
        default filename.
        """
        i = PPD()
        fh = StringIO('\x00\x01Hey\xff')
        obj_id = i.addFile(fh, None, {'filename': 'something.exe'})
        obj = i.getObject(obj_id)
        self.assertEqual(obj['filename'], 'something.exe')


    def test_addFile_filenameRequired(self):
        """
        filename must either be specified as an arg or in the metadata.
        """
        i = PPD()
        fh = StringIO('\x00\x01Hey\xff')
        self.assertRaises(ValueError, i.addFile, fh, None, {})


    def test_deleteFile(self):
        """
        When you delete a file's metadata, the content is also deleted.
        """
        i = PPD()
        fh = StringIO('\x00\x01Hey\xff')
        obj_id = i.addFile(fh, 'something.exe', {'hey': 'ho'})
        obj = i.getObject(obj_id)
        file_id = obj['_file_id']

        i.deleteObject(obj_id)
        self.assertRaises(KeyError, i.getFileContents, file_id)


    def test_multipleUsers(self):
        """
        Two instances of PPD using the same database should see
        the other guy's changes all the time.
        """
        dbfile = self.mktemp()
        a = PPD(dbfile)
        b = PPD(dbfile)
        a.addObject({'foo': 'bar'})
        self.assertEqual(len(b.listObjects()), 1,
            "Adding should be concurrent")

        a.updateObjects({'boo': 'hoo'})
        self.assertEqual(len(b.listObjects({'boo': 'hoo'})), 1,
            "Updating should be concurrent")

        a.deleteObject(1)
        self.assertEqual(len(b.listObjects()), 0,
            "Deleting should be concurrent")

        a.addFile(StringIO('foo'), 'foo.txt', {})
        self.assertEqual(len(b.listObjects()), 1,
            "Adding files should be concurrent")


class RuleBasedFileDumper_performActionTest(TestCase):


    def test_merge_yaml_firstTime(self):
        """
        You can write the object as YAML to a file.
        """
        tmpdir = FilePath(self.mktemp())
        reported = []
        obj = {
            '_id': 45,
            'name': 'hi',
            'guy': 'smiley',
        }
        dumper = RuleBasedFileDumper(tmpdir.path, reporter=reported.append)
        dumper.performAction({
            'merge_yaml': 'foo/bar/{_id}.yml',
        }, obj)

        yml = tmpdir.child('foo').child('bar').child('45.yml')
        self.assertTrue(yml.exists(), "Should have created the file")

        content = yaml.safe_load(yml.getContent())
        self.assertEqual(content, obj,
            "Should have written out the YAML data.")
        self.assertEqual(len(reported), 1, "Should have reported the write")


    def test_merge_yaml_noChange(self):
        """
        When merging YAML, if there's no change, don't report the file as
        having been written.
        """
        tmpdir = FilePath(self.mktemp())
        reported = []
        obj = {
            '_id': 45,
            'name': 'hi',
            'guy': 'smiley',
        }
        dumper = RuleBasedFileDumper(tmpdir.path, reporter=reported.append)
        dumper.performAction({
            'merge_yaml': 'foo/bar/{_id}.yml',
        }, obj)
        reported.pop()
        dumper.performAction({
            'merge_yaml': 'foo/bar/{_id}.yml',
        }, obj)

        yml = tmpdir.child('foo').child('bar').child('45.yml')
        self.assertTrue(yml.exists(), "Should have created the file")

        content = yaml.safe_load(yml.getContent())
        self.assertEqual(content, obj,
            "Should have written out the YAML data.")
        self.assertEqual(len(reported), 0,
            "Should not have reported the write because nothing changed.")


    def test_merge_yaml_change(self):
        """
        When merging YAML, if there's a change, report the file as
        having been written.
        """
        tmpdir = FilePath(self.mktemp())
        reported = []
        obj = {
            '_id': 45,
            'name': 'hi',
            'guy': 'smiley',
        }
        dumper = RuleBasedFileDumper(tmpdir.path, reporter=reported.append)
        dumper.performAction({
            'merge_yaml': 'foo/bar/{_id}.yml',
        }, obj)
        reported.pop()
        dumper.performAction({
            'merge_yaml': 'foo/bar/{_id}.yml',
        }, {
            '_id': 45,
            'name': 'bob',
        })

        yml = tmpdir.child('foo').child('bar').child('45.yml')
        self.assertTrue(yml.exists(), "Should have created the file")

        content = yaml.safe_load(yml.getContent())
        self.assertEqual(content, {
            '_id': 45,
            'name': 'bob',
            'guy': 'smiley',
        },
            "Should have written out the combined YAML data: %r" % (content,))
        self.assertEqual(len(reported), 1,
            "Should have reported the write because something changed.")


    def test_write_file(self):
        """
        You can write file contents out for file objects.
        """
        ppd = PPD()
        obj_id = ppd.addFile(StringIO('foo bar'), 'joe.txt', {'meta': 'data'})
        obj = ppd.getObject(obj_id)

        tmpdir = FilePath(self.mktemp())
        reported = []
        dumper = RuleBasedFileDumper(tmpdir.path, ppd=ppd,
            reporter=reported.append)
        dumper.performAction({
            'write_file': 'foo/bar/{filename}',
        }, obj)

        exp = tmpdir.child('foo').child('bar').child('joe.txt')
        self.assertTrue(exp.exists(), "Should make the file")
        self.assertEqual(exp.getContent(), 'foo bar')

        self.assertEqual(len(reported), 1,
            "Should have reported the write because something changed")


    def test_write_file_noChange(self):
        """
        Should not write the file the second time if nothing changed.
        """
        ppd = PPD()
        obj_id = ppd.addFile(StringIO('foo bar'), 'joe.txt', {'meta': 'data'})
        obj = ppd.getObject(obj_id)

        tmpdir = FilePath(self.mktemp())
        reported = []
        dumper = RuleBasedFileDumper(tmpdir.path, ppd=ppd,
            reporter=reported.append)
        dumper.performAction({
            'write_file': 'foo/bar/{filename}',
        }, obj)
        reported.pop()
        dumper.performAction({
            'write_file': 'foo/bar/{filename}',
        }, obj)

        self.assertEqual(len(reported), 0,
            "Should not have reported the write because nothing changed")


    def test_write_file_change(self):
        """
        Should write the file again if it changed.
        """
        ppd = PPD()
        id1 = ppd.addFile(StringIO('foo bar'), 'joe.txt', {'meta': 'a'})
        id2 = ppd.addFile(StringIO('baz who'), 'joe.txt', {'meta': 'b'})
        
        obj1 = ppd.getObject(id1)
        obj2 = ppd.getObject(id2)

        tmpdir = FilePath(self.mktemp())
        reported = []
        dumper = RuleBasedFileDumper(tmpdir.path, ppd=ppd,
            reporter=reported.append)
        dumper.performAction({
            'write_file': 'foo/bar/{filename}',
        }, obj1)
        reported.pop()
        dumper.performAction({
            'write_file': 'foo/bar/{filename}',
        }, obj2)

        exp = tmpdir.child('foo').child('bar').child('joe.txt')
        self.assertTrue(exp.exists())
        self.assertEqual(exp.getContent(), 'baz who')
        self.assertEqual(len(reported), 1,
            "Should have reported the write because something changed")


class PPD_last_updatedTest(TestCase):

    def fakeLastUpdated(self, ppd, what):
        ppd.kv['sys:last_updated'] = str(what)


    def test_implementation(self):
        """
        Make sure the following tests are faking the right stuff.
        """
        ppd = PPD()
        self.fakeLastUpdated(ppd, 65.0)
        self.assertEqual(ppd.last_updated(), 65.0)

    def test_addObject(self):
        """
        Adding an object should change last_updated
        """
        ppd = PPD()
        self.fakeLastUpdated(ppd, 12.2)
        ppd.addObject({'foo': 'bar'})
        self.assertNotEqual(ppd.last_updated(), 12.2)

    def test_updateObject(self):
        """
        Updating an object should change last_updated.
        """
        ppd = PPD()
        ppd.addObject({'foo': 'bar'})
        self.fakeLastUpdated(ppd, 12.2)
        ppd.updateObjects({'foo': 'baz'})
        self.assertNotEqual(ppd.last_updated(), 12.2)

    def test_addFile(self):
        """
        Adding a file should update last_updated.
        """
        ppd = PPD()
        self.fakeLastUpdated(ppd, 12.2)
        ppd.addFile(StringIO('foo'), 'jim.txt', {})
        self.assertNotEqual(ppd.last_updated(), 12.2)

    def test_deleteObject(self):
        """
        Deleting an object is an update
        """
        ppd = PPD()
        ppd.addObject({'foo': 'bar'})
        self.fakeLastUpdated(ppd, 12.2)
        ppd.deleteObject(0)
        self.assertNotEqual(ppd.last_updated(), 12.2)


class RuleBasedFileDumper_dumpObjectTest(TestCase):


    def test_firstMatch(self):
        """
        Should act on the first match only.
        """
        tmpdir = FilePath(self.mktemp())
        rules = [
            {
                'pattern': {
                    'foo': '*',
                },
                'actions': [
                    {'merge_yaml': '{foo}.yml'},
                ]
            },
            {
                'pattern': {
                    'bar': '*',
                },
                'actions': [
                    {'merge_yaml': '{bar}.yml'},
                ]
            }
        ]
        dumper = RuleBasedFileDumper(tmpdir.path, rules)
        dumper.dumpObject({
            'foo': 'thefoo',
        })
        self.assertTrue(tmpdir.child('thefoo.yml').exists(),
            "Should have matched and acted on the first rule")
        dumper.dumpObject({
            'bar': 'hey',
        })
        self.assertTrue(tmpdir.child('hey.yml').exists(),
            "Should have matched and acted on the second rule")
        self.assertEqual(len(tmpdir.children()), 2, "Should only have made "
            "the 2 expected files")


    def test_catchAll(self):
        """
        Everything should match a catchall rule.
        """
        tmpdir = FilePath(self.mktemp())
        rules = [
            {
                'pattern': {
                    'foo': '*',
                },
                'actions': [
                    {'merge_yaml': '{foo}.yml'},
                ]
            },
            {
                'pattern': 'all',
                'actions': [
                    {'merge_yaml': 'extra.yml'},
                ]
            }
        ]
        dumper = RuleBasedFileDumper(tmpdir.path, rules)
        dumper.dumpObject({
            'foo': 'thefoo',
        })
        self.assertTrue(tmpdir.child('thefoo.yml').exists(),
            "Should have matched and acted on the first rule")
        dumper.dumpObject({
            'bar': 'hey',
        })
        self.assertTrue(tmpdir.child('extra.yml').exists(),
            "Should have matched and acted on the second rule")
        self.assertEqual(len(tmpdir.children()), 2, "Should only have made "
            "the 2 expected files")


    def test_multipleActions(self):
        """
        Multiple actions can be specified for each rule.  Each action should
        happen.
        """
        tmpdir = FilePath(self.mktemp())
        rules = [
            {
                'pattern': {
                    'foo': '*',
                },
                'actions': [
                    {'merge_yaml': '{foo}.yml'},
                    {'merge_yaml': '{foo}2.yml'},
                ]
            },
        ]
        dumper = RuleBasedFileDumper(tmpdir.path, rules)
        dumper.dumpObject({
            'foo': 'thefoo',
        })
        self.assertTrue(tmpdir.child('thefoo.yml').exists(),
            "Should have matched and acted on the first rule first action")
        self.assertTrue(tmpdir.child('thefoo2.yml').exists(),
            "Should have matched and acted on the first rule second action")



class PPD_autoDumpTest(TestCase):


    def setUp(self):
        self.tmpdir = FilePath(self.mktemp())
        rules = [
            {
                'pattern': {
                    'foo': '*',
                },
                'actions': [
                    {'merge_yaml': '{foo}.yml'},
                ],
            },
            {
                'pattern': {
                    '_file_id': '*',
                },
                'actions': [
                    {'write_file': '{filename}'},
                ]
            }
        ]
        self.reported = []
        self.ppd = PPD(':memory:',
            RuleBasedFileDumper(self.tmpdir.path,
                                rules=rules,
                                reporter=self.reported.append),
            auto_dump=True)

    
    def test_addObject(self):
        """
        If you add an object, and auto-dumping is enabled, it should dump.
        """
        self.ppd.addObject({'foo': 'hey'})
        self.assertTrue(self.tmpdir.child('hey.yml').exists(),
            "Should have run the rules")
        self.assertEqual(len(self.reported), 1,
            "Should have reported a change")


    def test_addFile(self):
        """
        If you add a file, and auto-dumping is enabled, it should dump.
        """
        self.ppd.addFile(StringIO('foo bar'), 'guys.txt', {'x': 'x'})
        self.assertTrue(self.tmpdir.child('guys.txt').exists(),
            "Should have run the rules to create the file")
        self.assertEqual(len(self.reported), 1,
            "Should have reported the change")


    def test_updateObjects(self):
        """
        If you update some objects, and auto-dumping is enabled,
        it should dump.
        """
        self.ppd.addObject({'foo': 'hey'})
        self.reported.pop()
        self.ppd.updateObjects({'foo': 'woo'})

        self.assertTrue(self.tmpdir.child('woo.yml').exists(),
            "Should have run the rules")
        self.assertEqual(len(self.reported), 1,
            "Should have reported a change")

