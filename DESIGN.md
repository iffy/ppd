
# Filesystem #

We'll assume that the mount point is `/`.

There is a configuration file at `/_config.yml` which is stored in the object
database and is a YAML file which specifies the behavior of the filesystem.

It might look like this:

```yaml
files:
    # The following means that there will be a file named `/in` that when
    # written to will be piped to `ppo -f yaml` and then fed into the object
    # database.
    # The file is write-only.
    
    - path: '/in'
      in_script: 'ppo -f yaml'

    # The following means that there will be a file named `/yaml` that is
    # read-only and contains the output of the object database (as YAML)
    # piped to `ppo -f grep`

    - path: '/grep'
      out_script: 'ppo -f grep'

    - path: '/proofs/{host}-proof.txt'
      filter:
        filename: 'proof.txt'
      type: file

    # This does a few things.  It identifies that there are certain objects
    # identified by these keys:
    #   - host
    #   - host,port
    #   - host,vuln
    #
    # So if there is an object with a `host` key and it doesn't have a
    # `port` or `vuln` key, its metadata will end up in
    # `/host/{host}/_meta.yml`

    - path: '/host/{host}'
      type: obj
      children:
        - path: '/port/{port}'
          type: obj
        - path: '/vuln/{vuln}'
          type: obj

    # This will make a directory with no `_meta.yml` and all
    # files in it have metadata `{"tag": "report"}`.

    - path: '/report'
      filter:
        tag: report

    # This makes a sub-directory in /report/vuln for each
    # host-vuln pair
    
    - path: '/report/vuln/{host}-{vuln}'
      type: obj

```



/_config.yml
/in
/yaml
/proofs/
    10.0.0.5-proof.txt
    127.0.0.1-proof.txt
/host/10.0.0.5
    _meta.yml
    file1.png
    file2.png
    proof.txt
    /port/80/
        _meta.yml
    /port/443/
        _meta.yml
    /vuln/
        smb/
            _meta.yml
            report.md
            screenshot.png
        ssh/
            _meta.yml
            report.md
            screenshot.png
/report/
    intro.md
    vulns.md
    vuln/
        10.0.0.5-smb/
            _meta.yml
            report.md
            screenshot.png
        10.0.0.5-ssh/
            ...
