#!/usr/bin/env python
# Copyright (c) The ppd team
# See LICENSE for details.


from distutils.core import setup

setup(
    name='ppd',
    version='0.1.0',
    description='Organizes pentesting information',
    author='Matt Haggard',
    author_email='haggardii@gmail.com',
    url='https://github.com/iffy/ppd',
    packages=[
        'ppd', 'ppd.test',
    ],
    install_requires=[
        'structlog',
        'unqlite',
        'PyYaml',
        'ordereddict',
    ],
    scripts=[
        'scripts/ppd',
    ]
)