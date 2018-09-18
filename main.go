package main

import (
	"fmt"
	"log"
	"net/http"
    "os"
    //"strconv"
    "github.com/op/go-logging"
    //"github.com/boltdb/bolt"

)

var logs = logging.MustGetLogger("example")
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



func main() {
    loginit()

	h := http.NewServeMux()

	h.HandleFunc("/push", func(w http.ResponseWriter, r *http.Request) {
        //{"ts": 1537242577, "metrics":"cpu", "host":"", "value":""}
	})

	h.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		fmt.Fprintln(w, "404")
	})
	logs.Notice("Listen :8080")
    err := http.ListenAndServe(":8080", logger(h))
	logs.Fatal(err)
}

