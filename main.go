package main

import (
	"fmt"
	"log"
	"net/http"
    "os"
    "strconv"
    "time"
    "encoding/binary"
    "github.com/op/go-logging"
    "github.com/boltdb/bolt"

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

func logger(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logs.Info(fmt.Sprintf("%s requested %s", r.RemoteAddr, r.URL))
		h.ServeHTTP(w, r)
	})
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

func push(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()

    ts, err := strconv.ParseInt(r.FormValue("ts"), 10, 64)
    if err != nil {
        log.Fatalln("ts atoi error !")
    }

    value, err := strconv.ParseFloat(r.FormValue("value"), 64)
    if err != nil {
        log.Fatalln("value atoi error !")
    }

    metric := r.FormValue("metric")
    host := r.FormValue("host")

    
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


    v, exists := m[dbpath]
    if (exists == false) {
        db, err := bolt.Open(dbpath, 0600, nil)
        if err != nil {
            fmt.Errorf("create bucket: %s", err)
        }
        m[dbpath] = db
    }
    m[dbpath].Update(func(tx *bolt.Tx) error {
        b, err := tx.CreateBucketIfNotExists([]byte(metric))
        if err != nil {
            return fmt.Errorf("create bucket: %s", err)
        }
        Use(b)
        return nil
    })
    m[dbpath].Update(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(metric))
        bts := make([]byte, 8)
        bvalue := make([]byte, 8)
        binary.LittleEndian.PutUint64(bts, uint64(ts))
        binary.LittleEndian.PutUint64(bvalue, uint64(value))
        err := b.Put(bts, bvalue)
        if err != nil {
            fmt.Errorf("push bucket: %s", err)
        }
        Use(b)
        return nil
    })

    Use(metric, host, value, v)
}

func defaults(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(404)
    fmt.Fprintln(w, "404")
}

func main() {
    loginit()
	h := http.NewServeMux()

	h.HandleFunc("/push", push)
	h.HandleFunc("/", defaults)

	logs.Notice("Listen :8080")
    err := http.ListenAndServe(":8080", logger(h))
	logs.Fatal(err)
}

