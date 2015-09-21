from twisted.trial.unittest import TestCase
from twisted.internet import protocol, defer, reactor, utils
from twisted.python import log
from twisted.python.procutils import which
from twisted.python.filepath import FilePath

import sys
import time
import yaml

from StringIO import StringIO

from ppd.util import PPD

skip_fuse = ''
try:
    import fuse
    fuse
except ImportError:
    skip_fuse = 'You must install fusepy in order to run this test.'

unmount = which('umount')[0]


class ProcessProtocol(protocol.ProcessProtocol):

    exitCode = None

    def __init__(self):
        self.done = defer.Deferred()
        self.started = defer.Deferred()
        self.output_started = defer.Deferred()

    def connectionMade(self):
        self.started.callback(self)

    def outReceived(self, data):
        log.msg(data, system='out')

    def errReceived(self, data):
        if 'ready' in data and not self.output_started.called:
            self.output_started.callback(self)
        log.msg(data, system='err')

    def processEnded(self, status):
        log.msg('prcessEnded: %r' % (status,))
        self.exitCode = status.value.exitCode
        self.done.callback(self)

    def terminate(self):
        log.msg('sending kill')
        self.transport.signalProcess('KILL')
        return self.done


class FileSystemTest(TestCase):

    skip = skip_fuse
    timeout = 5

    def _rawStartFS(self, dbfile, mountpoint, layout_path):
        proto = ProcessProtocol()
        all_args = ['python', '-m', 'ppd.runner',
            '-d', dbfile,
            'fs', mountpoint, layout_path]
        log.msg('spawn: %r' % (all_args,))
        reactor.spawnProcess(proto, sys.executable, all_args,
                env={'PYTHONPATH': '..'})
        self.addCleanup(self.unmountPath, mountpoint)
        self.addCleanup(proto.terminate)
        return proto.started

    def unmountPath(self, mountpoint):
        log.msg('unmounting: %r' % (mountpoint,))
        return utils.getProcessOutput(unmount, [mountpoint])

    @defer.inlineCallbacks
    def startFS(self, layout):
        self.dbfile = dbfile = self.mktemp()
        log.msg('dbfile: %s' % (self.dbfile,))
        mountpoint = FilePath(self.mktemp())
        log.msg('mountpoint: %s' % (mountpoint.path,))
        layout_file = FilePath(self.mktemp())
        log.msg('layout: %s' % (layout_file.path,))
        layout_file.setContent(yaml.safe_dump(layout))
        yield self._rawStartFS(dbfile, mountpoint.path, layout_file.path)
        while not mountpoint.exists():
            time.sleep(0.5)
        time.sleep(0.5)
        defer.returnValue(mountpoint)


    def ppd(self):
        return PPD(self.dbfile)


    @defer.inlineCallbacks
    def test_scriptable(self):
        mountpoint = yield self.startFS({
            'paths': [
                {
                    'path': 'cattable',
                    'scriptable': {
                        'out_script': 'cat',
                    },
                },
            ],
        })
        
        cattable = mountpoint.child('cattable')
        self.assertEqual(cattable.getContent(), '[]\n')

        # add some data to the database
        ppd = self.ppd()
        ppd.addObject({'foo': 'bar'})
        self.assertEqual(cattable.getContent(), '- _id: 1\n'
                                                '  foo: bar\n')


    @defer.inlineCallbacks
    def test_objfiles(self):
        """
        You can have a directory that contains all the files related
        to a particular key.
        """
        mountpoint = yield self.startFS({
            'paths': [
                {
                    'path': 'hosts',
                    'objdir': {
                        'keys': ['host'],
                        'display': '{host}',
                    },
                },
            ],
        })

        hosts = mountpoint.child('hosts')
        self.assertEqual(len(hosts.children()), 0, "Should start "
            "empty")

        # add a file
        ppd = self.ppd()
        ppd.addFile(StringIO('foo'), 'foo.txt', {'host': 'foo.com'})
        
        foo_com = hosts.child('foo.com')
        self.assertTrue(foo_com.isdir(), "Should have made a foo.com directory")
        self.assertEqual(foo_com.child('foo.txt').getContent(), 'foo',
            "Should have the foo.txt file available")

