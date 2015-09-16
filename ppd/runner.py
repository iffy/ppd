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

def read(args):
    i = PPDInterface(args.database)
    data = yaml.safe_load(sys.stdin)
    if isinstance(data, dict):
        data = [data]
    i.addRecords(data)

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
# in
#--------------------------------
p = cmds.add_parser('in')
p.set_defaults(func=read)


#--------------------------------
#
#--------------------------------


def run():
    args = ap.parse_args()

    if args.verbose:
        structlog.configure(logger_factory=structlog.PrintLoggerFactory(sys.stderr))
    else:
        structlog.configure(logger_factory=structlog.ReturnLoggerFactory())

    args.func(args)
