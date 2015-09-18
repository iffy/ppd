# Copyright (c) The ppd team
# See LICENSE for details.

from twisted.trial.unittest import TestCase
from twisted.python import log
from twisted.python.filepath import FilePath
from StringIO import StringIO


from ppd.runner import run



class runTest(TestCase):


    def setUp(self):
        self.dbfile = self.mktemp()


    def runWithDatabase(self, args, stdin=''):
        stdout = StringIO()
        stderr = StringIO()
        stdin = StringIO(stdin)
        run(['--database', self.dbfile] + args, stdin=stdin, stdout=stdout, stderr=stderr)
        stdout = stdout.getvalue()
        stderr = stderr.getvalue()
        log.msg('run: %r\nstdout: %r\nstderr: %r' % (args, stdout, stderr))
        return stdout, stderr


    def test_everything(self):
        # add
        self.runWithDatabase(['add', 'foo:bar'])

        # list
        stdout, stderr = self.runWithDatabase(['list'])
        self.assertEqual(stderr, '')
        self.assertTrue('foo' in stdout, stdout)

        # attach
        tmpfile = FilePath(self.mktemp())
        tmpfile.setContent('hey guys\x00')
        self.runWithDatabase(['attach', '-f', tmpfile.path])

        # update
        self.runWithDatabase(['update', 'who:haw'])
        stdout, _ = self.runWithDatabase(['list'])
        self.assertEqual(stdout.count('haw'), 2,
            "Should have updated both records: %r" % (stdout,))

        # dump
        dmpdir = FilePath(self.mktemp())
        layout = FilePath(self.mktemp())
        layout.setContent(
            "rules:\n"
            "  - pattern:\n"
            "      foo: '*'\n"
            "    actions:\n"
            "      - merge_yaml: '{foo}.yml'\n")
        stdout, _ = self.runWithDatabase(['dump', '--layout', layout.path,
            '--dump-dir', dmpdir.path])
        self.assertNotEqual(stdout, '', "Should have some output")
        self.assertTrue(dmpdir.child('bar.yml').exists(),
            "Should have dumped")


