heka-hdfs
=========
HDFS Heka Output Plugin for [Mozilla Heka](http://hekad.readthedocs.org/)

HDFSOutput
==========
HDFSOutput writes the payload to HDFS after a configured interval or number messages.

Interpolate will replace variables in `path` with their corresponding field entries.  Variables are expected BASH-like format.  If the cooresponding field does not exist, it will be empty in the final path.  
This adds functionality similar to Logstash, Flume, and Flutends HDFS outputs.  

Example:
       The incoming pack has the following fields defined : Fields[server] = web01, Fields[date] = 2014-01-01
       If `path` is set to "/tmp/${date}/${server}.txt" and `interpolate` is set to true, the resulting `path` will become "hdfs:///tmp/2014-01-01/web01.txt".

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
- interpolate: Interpolate variables from fields.  (default false)

Example:

        [hdfs_output]
        type = "HDFSOutput"
        message_matcher = "Type == 'HDFS'"
        host = "localhost:14000"
        path = "/tmp/hdfs"
        extension = "txt"
        timestamp = true
        encoder="encoder_payload"

        The above configuration will output files in the following format /tmp/hdfs.<epochtime in ms>.txt
