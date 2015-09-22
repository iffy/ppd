from twisted.trial.unittest import TestCase
from twisted.internet import protocol, defer, reactor, utils, task
from twisted.python import log
from twisted.python.procutils import which
from twisted.python.filepath import FilePath

import sys
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
        log.msg('connection made')
        self.transport.closeStdin()
        self.started.callback(self)

    def outReceived(self, data):
        log.msg(data, system='stdout')

    def errReceived(self, data):
        if 'ready' in data and not self.output_started.called:
            self.output_started.callback(self)
        log.msg(data, system='stderr')

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
        
        ex = sys.executable
        all_args = ['python', '-u', '-m', 'ppd.runner',
            '-d', dbfile,
            'fs', mountpoint, layout_path]

        log.msg('spawn: %r' % (all_args,))
        reactor.spawnProcess(proto, ex, all_args,
                env={'PYTHONPATH': '..'},
                childFDs={0:'w', 1:'r', 2:'r'})
        self.addCleanup(self.unmountPath, mountpoint)
        self.addCleanup(proto.terminate)
        return proto.started

    def unmountPath(self, mountpoint):
        log.msg('unmounting: %r' % (mountpoint,))
        return utils.getProcessOutput(unmount, [mountpoint])

    def waitForFileToExist(self, fp, delay=0.05):
        d = defer.Deferred()
        def check(d, fp):
            if fp.exists():
                reactor.callLater(delay, d.callback, fp)
        lc = task.LoopingCall(check, d, fp)
        lc.start(delay*3)

        def cleanup(result, lc):
            lc.stop()
            return result
        d.addBoth(cleanup, lc)
        return d

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
        mountpoint = yield self.waitForFileToExist(mountpoint)
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
                        'display': '{host}',
                    },
                },
            ],
        })

        hosts = mountpoint.child('hosts')
        self.assertEqual(len(hosts.children()), 0, "Should start "
            "empty")


        # test ppd -> fs
        # attach a file
        ppd = self.ppd()
        ppd.addFile(StringIO('foo'), 'foo.txt', {'host': 'foo.com'})
        foo_com = hosts.child('foo.com')
        self.assertTrue(foo_com.isdir(), "Should have made a foo.com directory")
        self.assertEqual(foo_com.child('foo.txt').getContent(), 'foo',
            "Should have the foo.txt file available")

        # make a host
        ppd.addObject({'host': 'example.com'})
        self.assertTrue(hosts.child('example.com').isdir(),
            "Should have made directory because host exists")

        # test fs -> ppd
        # make a directory
        bar_com = hosts.child('bar.com')
        bar_com.makedirs()
        objects = ppd.listObjects({'host': 'bar.com'})
        self.assertEqual(len(objects), 1, "Should have made a bar.com host")
        
        # make a file
        bar_com.child('bar.txt').setContent('content of bar.txt')
        objects = ppd.listObjects({'filename': 'bar.txt'})
        self.assertEqual(len(objects), 1, "Should have made a bar.txt")
        ppd.getFileContents(objects[0]['_id'])

        # delete a file
        bar_com.child('bar.txt').remove()
        objects = ppd.listObjects({'filename': 'bar.txt'})
        self.assertEqual(len(objects), 0, "Should have deleted the file")

        # write a file
        fh = open(bar_com.child('bar2.txt').path, 'wb')
        fh.write('some data\n')
        fh.close()
        self.assertEqual(bar_com.child('bar2.txt').getContent(),
            'some data\n')
        objects = ppd.listObjects({'filename': 'bar2.txt'})
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0]['host'], 'bar.com')


    @defer.inlineCallbacks
    def test_multiple_objfiles(self):
        """
        You can have a directory that contains all the files related
        to a particular key.
        """
        mountpoint = yield self.startFS({
            'paths': [
                {
                    'path': 'hosts',
                    'objdir': {
                        'display': '{host}',
                    },
                },
                {
                    'path': 'ports',
                    'objdir': {
                        'display': '{port}',
                    }
                }
            ],
        })

        hosts = mountpoint.child('hosts')
        ports = mountpoint.child('ports')

        ppd = self.ppd()
        ppd.addFile(StringIO('foo'), 'data.txt',
            {'host': 'foo.com', 'port': '110'})

        self.assertTrue(hosts.child('foo.com').child('data.txt').exists())
        self.assertTrue(ports.child('110').child('data.txt').exists())

        ports.child('110').child('data.txt').setContent('new content')
        self.assertEqual(hosts.child('foo.com').child('data.txt').getContent(),
            'new content',
            'Changing content in one place should change it in both places.')

        ports.child('110').child('data.txt').remove()
        self.assertFalse(ports.child('110').child('data.txt').exists(),
            "ports/data.txt should no longer exist")
        self.assertFalse(ports.child('110').exists(),
            "ports/ should no longer exist")
        self.assertTrue(hosts.child('foo.com').child('data.txt').exists(),
            "Even though it was deleted in the ports/ dir, it"
            " should still exist in the hosts/ dirs")


