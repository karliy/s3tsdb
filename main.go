package main

import (
    "fmt"
    "log"
    "os"
    "strconv"
    "time"
    "math"
    "strings"
    "encoding/binary"
    "path/filepath"
    "github.com/op/go-logging"
    "github.com/boltdb/bolt"
    "github.com/valyala/fasthttp"
    "github.com/ks3sdklib/aws-sdk-go/aws"
    "github.com/ks3sdklib/aws-sdk-go/aws/credentials"
    "github.com/ks3sdklib/aws-sdk-go/service/s3"
    "github.com/ks3sdklib/aws-sdk-go/service/s3/s3manager"
)

func Use(vals ...interface{}) {
    for _, val := range vals {
        _ = val
    }
}

var logs = logging.MustGetLogger("example")

var m = make(map[string]*bolt.DB)

var dbbase = "/home/coding/tmp/"

var dbcache = "/home/coding/cache/"

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

    
    dbpath := dbbase + time.Unix(ts, 0).Format("2006/01/02")
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

func query(ctx *fasthttp.RequestCtx) {
    from, err := strconv.ParseInt(string(ctx.FormValue("from")), 10, 64)
    if err != nil {
        log.Fatalln("from atoi error !")
        ctx.Error("not found", fasthttp.StatusNotFound)
    }

    to, err := strconv.ParseInt(string(ctx.FormValue("to")), 10, 64)
    if err != nil {
        log.Fatalln("to atoi error !")
        ctx.Error("not found", fasthttp.StatusNotFound)
    }

    metric := ctx.FormValue("metric")
    host := string(ctx.FormValue("host"))
    ctx.SetStatusCode(fasthttp.StatusOK)
    ctx.SetBody([]byte("this is completely new body contents"))
    Use(from, to, metric, host)
}

func clearMkey() {
    for {
        now := time.Now()
        timeout := now.Add(-26 * time.Hour)
        for k, v := range m {
            adate := strings.Split(k, "-")
            t, err := time.Parse("2006/01/02", adate[len(adate) - 1])
            if err != nil {
                log.Fatalln("time parse error !")
            }
            if timeout.After(t) {
                v.Close()
                delete(m, k)
                logs.Warning(k)
            }
            Use(v)
        }
        time.Sleep(time.Hour)
    }
}

func expireddb(searchDir string) []string {
    fileList := []string{}
    err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
        if f.IsDir() {return nil}
        dbpath := path[len(searchDir):]

        if len(dbpath) < 11 {
            fmt.Printf("%s %s", "len path no s3tsdb", path)
            return nil
        }
        dbdate := dbpath[:10]
        dbname := dbpath[11:]
        dbtime, err := time.Parse("2006/01/02", dbdate)
        if err != nil {
            fmt.Printf("%s %s", "time parse no s3tsdb", path)
            return nil
        }
        if len(strings.Split(dbname, ".")) != 2 {
            fmt.Printf("%s %s", "split no s3tsdb", path)
            return nil
        }
        Use(dbname)
        now := time.Now()
        timeout := now.Add(-24 * 7 * time.Hour)
        if timeout.Before(dbtime) {
            return nil
        }
        fileList = append(fileList, dbpath)
        return nil
    })
    Use(err)
    return fileList
}

func dbmoveS3() {
    credentials := credentials.NewStaticCredentials("xxx","xxx","")
    s3Config := &aws.Config{
        Region: "BEIJING",
        Credentials: credentials,
        Endpoint:"ks3-cn-beijing.ksyun.com",//ks3地址
        DisableSSL:true,//是否禁用https
        LogLevel:0,//是否开启日志,0为关闭日志，1为开启日志
        S3ForcePathStyle:false,//是否强制使用path style方式访问
        LogHTTPBody:false,//是否把HTTP请求body打入日志
        Logger:nil,//打日志的位置
    }
    s := s3.New(s3Config)
    for {
        fileList := expireddb(dbbase)
        for _, file := range fileList {
            dbpath := dbbase + file
            mgr := s3manager.NewUploader(&s3manager.UploadOptions{
                S3: s,
                PartSize: 10 * 1024 * 1024,
                Concurrency: 3,
            })
            f, err  := os.Open(dbpath)
            if err != nil {
                fmt.Errorf("failed to open file %q, %v", dbpath, err)
                return
            }
            resp, err := mgr.Upload(&s3manager.UploadInput{
                Bucket: aws.String("s3tsdb"),
                Key:    aws.String(file),
                Body:   f,
                ContentType: aws.String("application/ocet-stream"),
                ACL: aws.String("private"),
                Metadata: map[string]*string{},
            })
            if err != nil {
                fmt.Printf("Failed to upload data %s\n", err.Error())
                return
            }
            Use(resp)
            os.Remove(dbpath)
        }
        //清理空文件夹
        time.Sleep(time.Hour)
    }
}

func main() {
    loginit()
    go clearMkey()
    go dbmoveS3()
    fhttp := func(ctx *fasthttp.RequestCtx) {
        logs.Debug(fmt.Sprintf("%s requested %s", ctx.RemoteAddr(), ctx.URI()))
        switch string(ctx.Path()) {
            case "/push":
                push(ctx)
            case "/query":
                query(ctx)
            default:
                ctx.Error("not found", fasthttp.StatusNotFound)
        }
    }
    fasthttp.ListenAndServe(":8080", fhttp)
}