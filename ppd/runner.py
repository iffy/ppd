#!/usr/bin/env python
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
# dumping
#--------------------------------
def _maybeDumpObjects(ppdi, args, objects):
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
    meta_glob = getMetadata(args)
    objects = i.listObjects(meta_glob)
    sys.stdout.write(yaml.safe_dump(objects, default_flow_style=False))

p = cmds.add_parser('list')
p.set_defaults(func=listObjects)

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

p.add_argument('--meta', '-m',
    action='append',
    dest='meta',
    type=kvpairs,
    help='Metadata associated with the files.'
         '  May be specified multiple times.'
         '  Should be of the format key:value')
p.add_argument('filenames', nargs='+')


#--------------------------------
# add
#--------------------------------
def addData(args):
    logger.msg('addData', args=args)
    i = PPDInterface(args.database)
    meta = getMetadata(args)
    obj_id = i.addObjects([meta])
    obj = i.objects.fetch(obj_id)
    _maybeDumpObjects(i, args, [obj])

p = cmds.add_parser('add',
    help='Create an object with the given metadata values',
    parents=[dump_parser])
p.set_defaults(func=addData)
p.add_argument('meta',
    nargs='+',
    type=kvpairs,
    help='Metadata to be stored.'
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
