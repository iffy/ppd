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
        self.assertEqual(obj['__id'], object_id)


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
            '__id': id1,
            'foo': 'bar',
            'A': 'A',
        })
        self.assertEqual(objects[1], {
            '__id': id2,
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
            '__id': id1,
            'foo': 'bar',
            'A': 'A',
        })

        objects = i.listObjects()
        self.assertEqual(objects[1], {
            '__id': id2,
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
            '__id': id1,
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
            "Should return the contents provided when attaching the file")


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


class RuleBasedFileDumper_performActionTest(TestCase):


    def test_merge_yaml_firstTime(self):
        """
        You can write the object as YAML to a file.
        """
        tmpdir = FilePath(self.mktemp())
        reported = []
        obj = {
            '__id': 45,
            'name': 'hi',
            'guy': 'smiley',
        }
        dumper = RuleBasedFileDumper(tmpdir.path, reporter=reported.append)
        dumper.performAction({
            'merge_yaml': 'foo/bar/{__id}.yml',
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
            '__id': 45,
            'name': 'hi',
            'guy': 'smiley',
        }
        dumper = RuleBasedFileDumper(tmpdir.path, reporter=reported.append)
        dumper.performAction({
            'merge_yaml': 'foo/bar/{__id}.yml',
        }, obj)
        reported.pop()
        dumper.performAction({
            'merge_yaml': 'foo/bar/{__id}.yml',
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
            '__id': 45,
            'name': 'hi',
            'guy': 'smiley',
        }
        dumper = RuleBasedFileDumper(tmpdir.path, reporter=reported.append)
        dumper.performAction({
            'merge_yaml': 'foo/bar/{__id}.yml',
        }, obj)
        reported.pop()
        dumper.performAction({
            'merge_yaml': 'foo/bar/{__id}.yml',
        }, {
            '__id': 45,
            'name': 'bob',
        })

        yml = tmpdir.child('foo').child('bar').child('45.yml')
        self.assertTrue(yml.exists(), "Should have created the file")

        content = yaml.safe_load(yml.getContent())
        self.assertEqual(content, {
            '__id': 45,
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
        self.ppd = PPD(':mem:',
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

