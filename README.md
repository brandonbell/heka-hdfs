heka-hdfs
=========
HDFS Heka Output Plugin for [Mozilla Heka](http://hekad.readthedocs.org/)

HDFSOutput
==========
HDFSOutput writes the payload to HDFS after a configured interval or number messages.

Config:
- host: Host and port of the HTTPfs or WebHDFS instance. (default localhost:14000)
- user: Username to create create connection with.
- timeout: Timeout for HDFS connection in seconds. (default 15 seconds)
- keepalive: Disable HDFS connection keepalive. (default false)
- path: Full output file path on HDFS.
- timestamp: Append epochtime in milliseconds to filename. (default false)
- extension: String to append to end of path(+timestamp).  This can be used to denote filetype.  
- perm: Permissions of file on HDFS. (default 0700)
- overwrite: Overwrite file if it already exists. (default false)
- blocksize: HDFS blocksize in bytes. (default 134217728 (128MB))
- replication: HDFS replication facter of file. (default 3)
- buffersize: Size of buffer used in transferring data to HDFS. (default 4096)

Example:

        [hdfs_output]
        type = "HDFSOutput"
        message_matcher = "Type == 'HDFS'"
        host = "localhost:14000"
        path = "/tmp/hdfs.output"
        encoder="encoder_payload"
