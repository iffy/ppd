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

p = cmds.add_parser('import')
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
            i.addFile(fh, filename, meta)
            logger.msg('Attached', filename=filename, meta=meta)


p = cmds.add_parser('attach',
    help='Add a file with some associated metadata')
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
    i.addObjects([meta])

p = cmds.add_parser('set',
    help='Update/Create an object with the given metadata values')
p.set_defaults(func=addData)

p.add_argument('meta',
    nargs='+',
    type=kvpairs,
    help='Metadata to be stored.'
         '  Should be of the format key:value')


def run():
    args = ap.parse_args()

    if args.verbose:
        structlog.configure(logger_factory=structlog.PrintLoggerFactory(sys.stderr))
    else:
        structlog.configure(logger_factory=structlog.ReturnLoggerFactory())

    args.func(args)
