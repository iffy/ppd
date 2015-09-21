# Copyright (c) The ppd team
# See LICENSE for details.

import sys
import os
import argparse
import yaml

import structlog
logger = structlog.get_logger()

from ppd.util import PPD, RuleBasedFileDumper

def kvpairs(s):
    return [x.strip() for x in s.split(':', 1)]

def getMetadata(args):
    return dict(args.meta)

def getPPD(args):
    dump_dir = None
    if 'dump_dir' in args:
        dump_dir = args.dump_dir
    layout = None
    rules = None
    if 'layout' in args and args.layout:
        with open(args.layout, 'rb') as fh:
            layout = yaml.safe_load(fh)
            rules = layout['rules']
    auto_dump = all([dump_dir, rules])
    def reporter(x):
        args.stdout.write(x + '\n')
    return PPD(args.database,
        dumper=RuleBasedFileDumper(dump_dir, rules, reporter=reporter),
        auto_dump=auto_dump)


def getFilter(args):
    return dict(args.filter)

filter_parser = argparse.ArgumentParser(add_help=False)
filter_parser.add_argument('--filter', '-f',
    default=[],
    action='append',
    type=kvpairs,
    help='Metadata to filter by.'
         '  Should be of the format key:glob_pattern')

dump_parser = argparse.ArgumentParser(add_help=False)
dump_parser.add_argument('--dump-dir', '-D',
    default=os.environ.get('PPD_DUMP_DIRECTORY', None),
    help='If provided, dump to the given directory.'
         '  --layout/-L/PPD_LAYOUT must also be specified.'
         '  You may set the PPD_DUMP_DIRECTORY env var, too.'
         '  (current default: %(default)s)')
dump_parser.add_argument('--layout', '-L',
    default=os.environ.get('PPD_LAYOUT', None),
    help='YAML file indicating how things should be dumped.'
         '  You may also set the PPD_LAYOUT env var.'
         '  (current default: %(default)s)')


ap = argparse.ArgumentParser(
    description="Useful for organizing output related to pentesting")
ap.add_argument('--verbose', '-v', action='store_true',
    help='Provide verbose logging output')
ap.add_argument('--database', '-d',
    default=os.environ.get('PPD_DATABASE', 'theppd.db'),
    help='Database file to use.'
         '  Can be specified with PPD_DATABASE env var.'
         ' (current default: %(default)s)')
ap.set_defaults(stdout=sys.stdout, stderr=sys.stderr, stdin=sys.stdin)

cmds = ap.add_subparsers(dest='command', help='command help')

#--------------------------------
# import / read
#--------------------------------
def read(args):
    ppd = getPPD(args)
    data = yaml.safe_load(args.stdin)
    if isinstance(data, dict):
        data = [data]
    ppd.addObject(data)
    ppd.close()

p = cmds.add_parser('import',
    help='Import YAML objects from stdin',
    parents=[dump_parser])
p.set_defaults(func=read)


#--------------------------------
# list
#--------------------------------
def listObjects(args):
    ppd = getPPD(args)
    meta_glob = getFilter(args)
    objects = ppd.listObjects(meta_glob)
    if args.id:
        for obj in objects:
            args.stdout.write(str(obj['__id'])+'\n')
    else:
        args.stdout.write(yaml.safe_dump(objects, default_flow_style=False))
    ppd.close()

p = cmds.add_parser('list',
    help='List objects matching certain criteria.',
    parents=[filter_parser])
p.set_defaults(func=listObjects)

p.add_argument('--id', '-i',
    action='store_true',
    default=False,
    help='Return only the object ids, one per line')


#--------------------------------
# add
#--------------------------------
def addObject(args):
    ppd = getPPD(args)
    meta = getMetadata(args)
    ppd.addObject(meta)
    ppd.close()

p = cmds.add_parser('add',
    help='Create an object with the given metadata values',
    parents=[dump_parser])
p.set_defaults(func=addObject)
p.add_argument('meta',
    nargs='+',
    type=kvpairs,
    help='Metadata to be stored.'
         '  Should be of the format key:value')

#--------------------------------
# get
#--------------------------------
def getObject(args):
    ppd = getPPD(args)
    ret = []
    for object_id in args.object_ids:
        ret.append(ppd.getObject(object_id))
    args.stdout.write(yaml.safe_dump(ret, default_flow_style=False))
    ppd.close()


p = cmds.add_parser('get',
    help='Get objects by their ids')
p.set_defaults(func=getObject)
p.add_argument('object_ids',
    type=int,
    nargs='+',
    help='ID of objects to get')

#--------------------------------
# update
#--------------------------------
def updateObjects(args):
    meta_glob = getFilter(args)
    data = getMetadata(args)
    ppd = getPPD(args)
    ppd.updateObjects(data, meta_glob)
    ppd.close()

p = cmds.add_parser('update',
    help='Update objects',
    parents=[dump_parser, filter_parser])
p.set_defaults(func=updateObjects)

p.add_argument('meta',
    nargs='*',
    type=kvpairs,
    help='Metadata to filter by.'
         '  Should be of the format key:glob_pattern')

#--------------------------------
# rm
#--------------------------------
def deleteObjects(args):
    ppd = getPPD(args)
    for object_id in args.object_ids:
        ppd.deleteObject(object_id)
    ppd.close()

p = cmds.add_parser('rm',
    help='Delete objects',
    parents=[])
p.set_defaults(func=deleteObjects)

p.add_argument('object_ids',
    type=int,
    nargs='+',
    help='ID of objects to delete.')

#--------------------------------
# attach
#--------------------------------
def attachFile(args):
    ppd = getPPD(args)
    meta = getMetadata(args)
    if args.filenames:
        # read files from filesystem
        for filename in args.filenames:
            with open(filename, 'rb') as fh:
                ppd.addFile(fh, filename, meta)
    else:
        # stdin
        ppd.addFile(args.stdin, None, meta)
    ppd.close()


p = cmds.add_parser('attach',
    help='Add a file with some associated metadata',
    parents=[dump_parser])
p.set_defaults(func=attachFile)

p.add_argument('--file', '-f',
    dest='filenames',
    metavar='FILENAME',
    action='append',
    default=[],
    help="Can be specified multiple times to attach multiple files."
         " If no filenames are given, then a single file's contents"
         " will be read from stdin.  In this case, you must set the"
         " filename with METADATA (i.e. 'filename:bob.txt')")

p.add_argument('meta',
    nargs='*',
    metavar="METADATA",
    type=kvpairs,
    help='Metadata associated with the files.'
         '  Should be of the format key:value.'
         '  If no --file/-f is provided, contents will be read from stdin'
         '  and you must set filename here (i.e. "filename:bob.txt")')


#--------------------------------
# cat
#--------------------------------
def catFile(args):
    ppd = getPPD(args)
    meta_glob = getFilter(args)
    if meta_glob:
        for obj in ppd.listObjects(meta_glob):
            args.stdout.write(ppd.getFileContents(obj['_id']))
    for object_id in args.object_ids:
        args.stdout.write(ppd.getFileContents(object_id))
    ppd.close()

p = cmds.add_parser('cat',
    help='Print out the contents of a file',
    parents=[filter_parser])
p.set_defaults(func=catFile)

p.add_argument('object_ids',
    type=int,
    nargs='*',
    help='ID of files to get.  Note, this is the id of the file object,'
         ' NOT the _file_id within the file object')


#--------------------------------
# dump
#--------------------------------
def dumpData(args):
    ppd = getPPD(args)
    meta_glob = getFilter(args)
    ppd.dump(meta_glob)
    ppd.close()

p = cmds.add_parser('dump',
    help='Dump data to the filesystem',
    parents=[dump_parser, filter_parser])
p.set_defaults(func=dumpData)


#---------------------------------
# filesystem
#---------------------------------
def runFilesystem(args):
    from ppd.filesystem import getFileSystem
    from fuse import FUSE
    ppd = getPPD(args)
    layout = yaml.safe_load(open(args.fslayout, 'rb'))
    fs = getFileSystem(ppd, layout['paths'])
    if not os.path.exists(args.mountpoint):
        os.makedirs(args.mountpoint)
    FUSE(fs, args.mountpoint, direct_io=True, foreground=True)

p = cmds.add_parser('fs',
    help='Start a virtual filesystem for file-like access')
p.set_defaults(func=runFilesystem)

p.add_argument('mountpoint',
    help='Place to mount files')
p.add_argument('fslayout',
    help='YAML files containing mount layout definition')


def run(cmd_strings=None, stdout=sys.stdout, stderr=sys.stderr, stdin=sys.stdin):
    # fake out stdout/stderr

    args = ap.parse_args(cmd_strings)
    args.stdout = stdout
    args.stderr = stderr
    args.stdin = stdin

    if args.verbose:
        structlog.configure(logger_factory=structlog.PrintLoggerFactory(args.stderr))
    else:
        structlog.configure(logger_factory=structlog.ReturnLoggerFactory())

    logger.msg(args=args)
    args.func(args)


if __name__ == '__main__':
    run()
