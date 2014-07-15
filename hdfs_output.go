package hdfs_output

import (
    "os"
    "bytes"
    "errors"
    "fmt"
    . "github.com/mozilla-services/heka/pipeline"
    "github.com/gohadoop/webhdfs"
    "github.com/rafrombrc/go-notify"
    "sync"
    "time"
)

// Output plugin that writes message contents to a file on HDFS.
type HDFSOutput struct {
    *HDFSOutputConfig
    flushOpAnd bool
    fs         *webhdfs.FileSystem
    batchChan  chan []byte
    backChan   chan []byte
    timerChan  <-chan time.Time
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

    // Interval at which accumulated file data should be written to HDFS, in
    // milliseconds (default 1minute (60000)). Set to 0 to disable.
    FlushInterval uint32 `toml:"flush_interval"`

    // Number of messages to accumulate until file data should be written to
    // HDFS (default 1, minimum 1).
    FlushCount uint32 `toml:"flush_count"`

    // Operator describing how the two parameters "flush_interval" and
    // "flush_count" are combined. Allowed values are "AND" or "OR" (default is
    // "AND").
    FlushOperator string `toml:"flush_operator"`

    // Specifies whether or not Heka's stream framing will be applied to the
    // output. We do some magic to default to true if ProtobufEncoder is used,
    // false otherwise.
    UseFraming *bool `toml:"use_framing"`
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
        FlushInterval: 60000,
        FlushCount:    1,
        FlushOperator: "AND",
    }
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

    if conf.FlushCount < 1 {
        err = fmt.Errorf("Parameter 'flush_count' needs to be greater 1.")
        return
    }

    switch conf.FlushOperator {
    case "AND":
        hdfs.flushOpAnd = true
    case "OR":
        hdfs.flushOpAnd = false
    default:
        err = fmt.Errorf("Parameter 'flush_operator' needs to be either 'AND' or 'OR', is currently: '%s'",
            conf.FlushOperator)
        return
    }

    hdfs.batchChan = make(chan []byte)
    hdfs.backChan = make(chan []byte, 2) // Never block on the hand-back
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
func (hdfs *HDFSOutput) hdfsWrite(data []byte) (ok bool, err error) {
    if err = hdfs.hdfsConnection(); err != nil {
        panic(fmt.Sprintf("HDFSOutput unable to reopen HDFS Connection: %s", err))
    }
    ok, err = hdfs.fs.Create(
        bytes.NewReader(data),
        webhdfs.Path{Name: hdfs.Path},
        hdfs.Overwrite,
        hdfs.Blocksize,
        hdfs.Replication,
        hdfs.Perm,
        hdfs.Buffersize,
    )
    
    return
}    

func (hdfs *HDFSOutput) Run(or OutputRunner, h PluginHelper) (err error) {
    enc := or.Encoder()
    if enc == nil {
        return errors.New("Encoder required.")
    }
    if hdfs.UseFraming == nil {
        // Nothing was specified, we'll default to framing IFF ProtobufEncoder
        // is being used.
        if _, ok := enc.(*ProtobufEncoder); ok {
            or.SetUseFraming(true)
        }
    }
    var wg sync.WaitGroup
    wg.Add(2)
    go hdfs.receiver(or, &wg)
    go hdfs.committer(or, &wg)
    wg.Wait()
    return
}

// Runs in a separate goroutine, accepting incoming messages, buffering output
// data until the ticker triggers the buffered data should be put onto the
// committer channel.
func (hdfs *HDFSOutput) receiver(or OutputRunner, wg *sync.WaitGroup) {
    var (
        pack            *PipelinePack
        e               error
        timer           *time.Timer
        timerDuration   time.Duration
        msgCounter      uint32
        intervalElapsed bool
        outBytes        []byte
    )
    ok := true
    outBatch := make([]byte, 0, 10000)
    inChan := or.InChan()

    timerDuration = time.Duration(hdfs.FlushInterval) * time.Millisecond
    if hdfs.FlushInterval > 0 {
        timer = time.NewTimer(timerDuration)
        hdfs.timerChan = timer.C
    }

    for ok {
        select {
        case pack, ok = <-inChan:
            if !ok {
                // Closed inChan => we're shutting down, flush data
                if len(outBatch) > 0 {
                    hdfs.batchChan <- outBatch
                }
                close(hdfs.batchChan)
                break
            }
            if outBytes, e = or.Encode(pack); e != nil {
                or.LogError(e)
            } else {
                outBatch = append(outBatch, outBytes...)
                msgCounter++
            }
            pack.Recycle()

            // Trigger immediately when the message count threshold has been
            // reached if a) the "OR" operator is in effect or b) the
            // flushInterval is 0 or c) the flushInterval has already elapsed.
            // at least once since the last flush.
            if msgCounter >= hdfs.FlushCount {
                if !hdfs.flushOpAnd || hdfs.FlushInterval == 0 || intervalElapsed {
                    // This will block until the other side is ready to accept
                    // this batch, freeing us to start on the next one.
                    hdfs.batchChan <- outBatch
                    outBatch = <-hdfs.backChan
                    msgCounter = 0
                    intervalElapsed = false
                    if timer != nil {
                        timer.Reset(timerDuration)
                    }
                }
            }
        case <-hdfs.timerChan:
            if (hdfs.flushOpAnd && msgCounter >= hdfs.FlushCount) ||
                (!hdfs.flushOpAnd && msgCounter > 0) {

                // This will block until the other side is ready to accept
                // this batch, freeing us to start on the next one.
                hdfs.batchChan <- outBatch
                outBatch = <-hdfs.backChan
                msgCounter = 0
                intervalElapsed = false
            } else {
                intervalElapsed = true
            }
            timer.Reset(timerDuration)
        }
    }
    wg.Done()
}

// Runs in a separate goroutine, waits for buffered data on the committer
// channel, writes it out to HDFS, and puts the now empty buffer on
// the return channel for reuse.
func (hdfs *HDFSOutput) committer(or OutputRunner, wg *sync.WaitGroup) {
    initBatch := make([]byte, 0, 10000)
    hdfs.backChan <- initBatch
    var outBatch []byte
    var err error

    ok := true
    hupChan := make(chan interface{})
    notify.Start(RELOAD, hupChan)

    for ok { 
        outBatch, ok = <-hdfs.batchChan
        if !ok { 
            break
        }
        ok, err = hdfs.hdfsWrite(outBatch)
        if err != nil {
            or.LogError(fmt.Errorf("Can't write to %s: %s",  err, outBatch))
        }
        outBatch = outBatch[:0]
        hdfs.backChan <- outBatch
    }

    wg.Done()
}

func init() {
    RegisterPlugin("HDFSOutput", func() interface{} {
        return new(HDFSOutput)
    })
}
