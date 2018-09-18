package main

import (
	"fmt"
	"log"
    "os"
    "strconv"
    "time"
    "math"
    "encoding/binary"
    "github.com/op/go-logging"
    "github.com/boltdb/bolt"
    "github.com/valyala/fasthttp"

)

func Use(vals ...interface{}) {
    for _, val := range vals {
        _ = val
    }
}

var logs = logging.MustGetLogger("example")

var m = make(map[string]*bolt.DB)

type Password string
func (p Password) Redacted() interface{} {
	return logging.Redact(string(p))
}

func loginit() {
    var logformat = logging.MustStringFormatter(
        `%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`,
    )
    logFile, err  := os.Create("s3tsdb.log")
    defer logFile.Close()
    if err != nil {
        log.Fatalln("open file error !")
    }

	stdoutbackend := logging.NewLogBackend(os.Stderr, "", 0)
	logfilebackend := logging.NewLogBackend(logFile, "", 0)

    stdoutbackendFormatter := logging.NewBackendFormatter(stdoutbackend, logformat)
	logfilebackendFormatter := logging.NewBackendFormatter(logfilebackend, logformat)

    logging.SetBackend(stdoutbackendFormatter, logfilebackendFormatter)
}

func PathExists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil {
        return true, nil
    }
    if os.IsNotExist(err) {
        return false, nil
    }
    return false, err
}

func Int64ToByte(i int64) []byte {
    bits := uint64(i)
    bytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(bytes, bits)
    return bytes
}

func ByteToInt64(bytes []byte) int64 {
    bits := binary.LittleEndian.Uint64(bytes)
    return int64(bits)
}

func Float64ToByte(float float64) []byte {
    bits := math.Float64bits(float)
    bytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(bytes, bits)
    return bytes
}

func ByteToFloat64(bytes []byte) float64 {
    bits := binary.LittleEndian.Uint64(bytes)
    return math.Float64frombits(bits)
}

func push(ctx *fasthttp.RequestCtx) {
    ts, err := strconv.ParseInt(string(ctx.FormValue("ts")), 10, 64)
    if err != nil {
        log.Fatalln("ts atoi error !")
    }

    value, err := strconv.ParseFloat(string(ctx.FormValue("value")), 64)
    if err != nil {
        log.Fatalln("value atoi error !")
    }

    metric := ctx.FormValue("metric")
    host := string(ctx.FormValue("host"))

    
    dbpath := "/home/coding/tmp/" + time.Unix(ts, 0).Format("2006/01/02")
    exist, err := PathExists(dbpath)
    if err != nil {
        fmt.Printf("get dir error![%v]\n", err)
    }
    if (exist == false) {
        err := os.MkdirAll(dbpath, os.ModePerm)
        if err != nil {
            fmt.Printf("mkdir failed![%v]\n", err)
        }
    }
    dbpath = dbpath + "/" + host + ".db"

    mkey := host + "-" + time.Unix(ts, 0).Format("2006/01/02")
    v, exists := m[mkey]
    if (exists == false) {
        db, err := bolt.Open(dbpath, 0600, nil)
        if err != nil {
            fmt.Errorf("create bucket: %s", err)
        }
        m[mkey] = db
    }
    m[mkey].Update(func(tx *bolt.Tx) error {
        b, err := tx.CreateBucketIfNotExists(metric)
        if err != nil {
            return fmt.Errorf("create bucket: %s", err)
        }
        Use(b)
        return nil
    })
    m[mkey].Update(func(tx *bolt.Tx) error {
        b := tx.Bucket(metric)
        err := b.Put(Int64ToByte(ts), Float64ToByte(value))
        if err != nil {
            fmt.Errorf("push bucket: %s", err)
        }
        Use(b)
        return nil
    })
    logs.Info(fmt.Sprintf("%s %s %d %f", string(metric), host, ts, value))
    Use(metric, host, value, v)
}

func main() {
    loginit()
    fhttp := func(ctx *fasthttp.RequestCtx) {
        logs.Debug(fmt.Sprintf("%s requested %s", ctx.RemoteAddr(), ctx.URI()))
        switch string(ctx.Path()) {
            case "/push":
                push(ctx)
            default:
                ctx.Error("not found", fasthttp.StatusNotFound)
        }
    }
    fasthttp.ListenAndServe(":8080", fhttp)
}

