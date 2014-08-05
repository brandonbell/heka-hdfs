package hdfs

import (
    "os"
    "bytes"
    "errors"
    "fmt"
    . "github.com/mozilla-services/heka/pipeline"
    "github.com/gohadoop/webhdfs"
    "bitbucket.org/tebeka/strftime"
    "time"
    "strconv"
)

type HDFSOutput struct {
	*HDFSOutputConfig
        fs *webhdfs.FileSystem
}

func (hdfs *HDFSOutput) ConfigStruct() interface{} {
    return &HDFSOutputConfig{
        Host:          "localhost:14000",
        Timeout:       15,
        KeepAlive:     false,
        Perm:          0644,
        Overwrite:     false,
        Blocksize:     134217728,
        Replication:   3,
        Buffersize:    4096,
        Timestamp:     false, 
    }
}

// ConfigStruct for HDFSOutput plugin.
type HDFSOutputConfig struct {
    // WebHDFS or HTTPfs host and port (default localhost:14000)
    Host string `toml:"host"`

    // User to create connection with
    User string

    // Connection timeout in seconds to HDFS (default 15)
    Timeout uint `toml:"timeout"`

    // DisableKeepAlives (default false).
    KeepAlive bool `toml:"keepalive"`

    // Full output file path.
    Path string

    // Append epoch in milliseconds.  E.g. /<path>/<on>/<hdfs>/syslog.1407245278657
    Timestamp bool

    // Extension to append to "Path".  This can be used to denote filetype.  
    Extension string

    // Output file permissions (default "0700").
    Perm os.FileMode `toml:"perm"`

    // Overwrite HDFS file if exists (default false).
    Overwrite bool `toml:"overwrite"`

    // Blocksize (default 134217728, (128MB)).
    Blocksize uint64 `toml:"blocksize"`

    // Replication (default 3)
    Replication uint16 `toml:"replication"`

    // Size of the buffer used in transferring data (default 4096).
    Buffersize uint `toml:"buffersize"`

    // Specifies whether or not Heka's stream framing will be applied to the
    // output. We do some magic to default to true if ProtobufEncoder is used,
    // false otherwise.
    UseFraming *bool `toml:"use_framing"`
}

func (hdfs *HDFSOutput) Init(config interface{}) (err error) {
    conf := config.(*HDFSOutputConfig)
    hdfs.HDFSOutputConfig = conf

    // Allow setting of 0 to indicate default
    if conf.Blocksize < 0 {
        err = fmt.Errorf("Parameter 'blocksize' needs to be greater than 0.")
        return
    }
    if conf.Timeout < 0 {
        err = fmt.Errorf("Parameter 'timeout' needs to be greater than 0.")
        return
    }
    if conf.Replication < 0 {
        err = fmt.Errorf("Parameter 'replication' needs to be greater than 0.")
        return
    }
    if conf.Buffersize < 0 {
        err = fmt.Errorf("Parameter 'buffersize' needs to be greater than 0.")
        return
    }

    return
}

// Creates connection to HDFS.
func (hdfs *HDFSOutput) hdfsConnection() (err error) {
    conf := *webhdfs.NewConfiguration()
    conf.Addr = hdfs.Host
    conf.User = hdfs.User
    conf.ConnectionTimeout = time.Second * time.Duration(hdfs.Timeout)
    conf.DisableKeepAlives = hdfs.KeepAlive
    hdfs.fs, err = webhdfs.NewFileSystem(conf)
    return
}

// Writes to HDFS using go-webhdfs.Create
func (hdfs *HDFSOutput) hdfsWrite(data []byte) (err error) {
    if err = hdfs.hdfsConnection(); err != nil {
        panic(fmt.Sprintf("HDFSOutput unable to reopen HDFS Connection: %s", err))
    }

    path, err := strftime.Format(hdfs.Path, time.Now()); if err != nil {
        return
    }

    if hdfs.Timestamp == true { 
        now := time.Now().UnixNano()
        path = path + "." + strconv.FormatInt(now / 1e6, 10)
    }

    if hdfs.Extension != "" { 
        path = path + "." + hdfs.Extension
    }

    _, err = hdfs.fs.Create(
        bytes.NewReader(data),
        webhdfs.Path{Name: path},
        hdfs.Overwrite,
        hdfs.Blocksize,
        hdfs.Replication,
        hdfs.Perm,
        hdfs.Buffersize,
    )

    return
}

func (hdfs *HDFSOutput) Run(or OutputRunner, h PluginHelper) (err error) {	
    if or.Encoder() == nil {
        return errors.New("Encoder must be specified.")
    }

    var (
        e error
        outBytes []byte
    )
    inChan := or.InChan()

    for pack := range inChan {
        outBytes, e = or.Encode(pack)
	pack.Recycle()
        if e != nil {
            or.LogError(e)
            continue
        }
        if e = hdfs.hdfsWrite(outBytes); e != nil {
            or.LogError(e)
        }
    }

    return
}

func init() {
    RegisterPlugin("HDFSOutput", func() interface{} {
        return new(HDFSOutput)
    })
}

