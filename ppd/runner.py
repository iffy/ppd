# Copyright (c) The ppd team
# See LICENSE for details.

import sys
import os
import argparse
import yaml

import structlog
logger = structlog.get_logger()

from ppd.util import PPDInterface

def kvpairs(s):
    return [x.strip() for x in s.split(':', 1)]

def getMetadata(args):
    return {k:v for k,v in args.meta}

#--------------------------------
# filtering
#--------------------------------

def getFilter(args):
    return {k:v for k,v in args.filter}

filter_parser = argparse.ArgumentParser(add_help=False)
filter_parser.add_argument('--filter', '-f',
    default=[],
    action='append',
    type=kvpairs,
    help='Metadata to filter by.'
         '  Should be of the format key:glob_pattern')


#--------------------------------
# dumping
#--------------------------------
def _maybeDumpObjects(ppdi, args, objects):
    # XXX this should be rolled into PPDInterface and tested.
    if args.layout and args.dump_dir:
        layout = yaml.safe_load(open(args.layout, 'rb'))
        for obj in objects:
            ppdi.dumpObjectToFiles(args.dump_dir, layout, obj)

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

cmds = ap.add_subparsers(dest='command', help='command help')

#--------------------------------
# import / read
#--------------------------------
def read(args):
    i = PPDInterface(args.database)
    data = yaml.safe_load(sys.stdin)
    if isinstance(data, dict):
        data = [data]
    i.addObjects(data)

p = cmds.add_parser('import',
    help='Import YAML objects from stdin',
    parents=[dump_parser])
p.set_defaults(func=read)


#--------------------------------
# list
#--------------------------------
def listObjects(args):
    i = PPDInterface(args.database)
    meta_glob = getFilter(args)
    objects = i.listObjects(meta_glob)
    if args.id:
        for obj in objects:
            sys.stdout.write(str(obj['__id'])+'\n')
    else:
        sys.stdout.write(yaml.safe_dump(objects, default_flow_style=False))

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
    logger.msg('addObject', args=args)
    i = PPDInterface(args.database)
    meta = getMetadata(args)
    obj_id = i.addObjects([meta])
    obj = i.objects.fetch(obj_id)
    _maybeDumpObjects(i, args, [obj])

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
    logger.msg('get', args=args)
    i = PPDInterface(args.database)
    ret = []
    for object_id in args.object_ids:
        ret.append(i.getObject(object_id))
    sys.stdout.write(yaml.safe_dump(ret, default_flow_style=False))

p = cmds.add_parser('get',
    help='Get a objects by ids')
p.set_defaults(func=getObject)
p.add_argument('object_ids',
    type=int,
    nargs='+',
    help='ID of objects to get')

#--------------------------------
# update
#--------------------------------
def updateObjects(args):
    logger.msg('update', args=args)
    meta_glob = getFilter(args)
    data = getMetadata(args)
    i = PPDInterface(args.database)
    objects = i.updateObjects(data, meta_glob)
    _maybeDumpObjects(i, args, objects)

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
# attach
#--------------------------------
def attachFile(args):
    i = PPDInterface(args.database)
    meta = getMetadata(args)
    for filename in args.filenames:
        with open(filename, 'rb') as fh:
            obj_id = i.addFile(fh, filename, meta)
            obj = i.objects.fetch(obj_id)
            logger.msg('Attached', filename=filename, meta=meta)
            _maybeDumpObjects(i, args, [obj])


p = cmds.add_parser('attach',
    help='Add a file with some associated metadata',
    parents=[dump_parser])
p.set_defaults(func=attachFile)

p.add_argument('--file', '-f',
    dest='filenames',
    metavar='FILENAME',
    action='append',
    default=[],
    required=True,
    help='Can be specified multiple times to attach multiple files')

p.add_argument('meta',
    nargs='*',
    type=kvpairs,
    help='Metadata associated with the files.'
         '  Should be of the format key:value')



#--------------------------------
# dump
#--------------------------------
def dumpData(args):
    logger.msg('dump', args=args)
    i = PPDInterface(args.database)
    _maybeDumpObjects(i, args, i.listObjects())

p = cmds.add_parser('dump',
    help='Dump data to the filesystem',
    parents=[dump_parser])
p.set_defaults(func=dumpData)



def run():
    args = ap.parse_args()

    if args.verbose:
        structlog.configure(logger_factory=structlog.PrintLoggerFactory(sys.stderr))
    else:
        structlog.configure(logger_factory=structlog.ReturnLoggerFactory())

    args.func(args)
