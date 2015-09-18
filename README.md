[![Build Status](https://secure.travis-ci.org/iffy/ppd.png?branch=master)](http://travis-ci.org/iffy/ppd)

`ppd` is a command-line database, originally built for aiding a Penetration Tester.


# Installation #

    pip install git+https://github.com/iffy/ppd.git


# Basic Usage #

This explains basic CRUD operations, but the real reason you want to use `ppd` is for the [dump features](#dumping).

Get help with `ppd --help`

Add an object `{'host': '192.168.12.14', 'state':'up'}` to the database:

    ppd add host:192.168.12.14 state:up

List objects:

    ppd list

List object ids:

    ppd list --id

Get an object by its id:

    ppd get 0

List all objects with a `state` of `up`:

    ppd list -f state:up

Set `state` to `down` for all objects with a `host` of `192.168.12.14`:

    ppd update -f host:192.168.12.14 state:down

List all objects with a `host` of any kind:

    ppd list -f host:*


# Files #

Save a file `somefile.txt` with some associated metadata (`{'name':'bob'}` in this case):

    ppd attach -f somefile.txt name:bob

Print out the file:

    ppd cat -f name:bob

# Dumping #

`ppd` lets you dump the data out to your filesystem according to rules.  Here's a sample rule file (named `layout.yml`):

    rules:
      - pattern:
          host: '*'
        actions:
          - merge_yaml: '{host}/info.yml'
      - pattern:
          _file_id: '*'
        actions:
          - write_file: '{filename}'
          - merge_yaml: '{filename}.yml'

Dump the database out using the above layout:

    ppd dump -L layout.yml -D /tmp/dump

Which will produce the following:

    $ find /tmp/dump
    /tmp/dump
    /tmp/dump/192.168.12.14
    /tmp/dump/192.168.12.14/info.yml
    /tmp/dump/somefile.txt
    /tmp/dump/somefile.txt.yml

    $ cat /tmp/dump/192.168.12.14/info.yml
    __id: 0
    host: 192.168.12.14
    state: down


You can also make `ppd` automatically dump files according to rules by setting some environment variables.  For example:

    export PPD_DUMP_DIRECTORY="/tmp/dump"
    export PPD_LAYOUT="layout.yml"

Then, any time you add/update data, it will automatically be written out the file system simultaneously:

    $ ppd add host:10.0.0.5 state:up
    $ cat /tmp/dump/10.0.0.5/info.yml
    __id: 2
    host: 10.0.0.5
    state: up
