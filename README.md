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
- path: Full output file path on HDFS
- perm: Permissions of file on HDFS. (default 0700)
- overwrite: Overwrite file if it already exists. (default false)
- blocksize: HDFS blocksize in bytes. (default 134217728 (128MB))
- replication: HDFS replication facter of file. (default 3)
- buffersize: Size of buffer used in transferring data to HDFS. (default 4096)
- flush_interval: Interval in milliseconds data should be written to HDFS. (default 60000)
- flush_count: Number of messages toa ccumulate before data is written to HDFS. (default 1)
- flush_operator: Operator between flush_interval and flush_count. (default AND)

Example:
        [hdfs_output]
        type = "HDFSOutput"
        message_matcher = "Type == 'HDFS'"
        host = "localhost:14000"
        path = "/tmp/hdfs.output"
        encoder="encoder_payload"

TODO
=====
1. Add dynamic configuration for output path.
2. Add output compression.
