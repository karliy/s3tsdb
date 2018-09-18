package main
import (
    "net/http"
    "net/url"
    "io/ioutil"
    "fmt"
    "runtime"
)
func Use(vals ...interface{}) {
    for _, val := range vals {
        _ = val
    }
}

func tests3tsdb() {
    resp, err := http.PostForm("http://127.0.0.1:8080/push",
	url.Values{"ts": {"1531261558"}, "metric": {"cpu.load"}, "host": {"elk"}, "value": {"5.1"}})
    if err != nil {
        fmt.Printf("http error %s", err)
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    fmt.Println(string(body))
    runtime.Gosched()
}

func main() {
    for a := 0; a < 10; a++ {
        go tests3tsdb()
    }
}



