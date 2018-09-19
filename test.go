package main

import (
	"fmt"
	"time"
    "strconv"
    "net/http"
    "net/url"
    "math/rand"
    "io/ioutil"
)

func Use(vals ...interface{}) {
    for _, val := range vals {
        _ = val
    }
}

func tests3tsdb(host string) {
    resp, err := http.PostForm("http://127.0.0.1:8080/push",
    url.Values{"ts": {strconv.FormatInt(time.Now().Unix(), 10)}, "metric": {"cpu.load"}, "host": {host}, "value": {strconv.FormatFloat(rand.Float64(), 'f', 6, 64)}})
    if err != nil {
        fmt.Printf("http error %s", err)
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        fmt.Printf("http error %s", err)
    }
    Use(body)

}

func say(host string) {
	for {
		time.Sleep(time.Second)
        tests3tsdb(host)
	}
}

func main() {
    for i :=1; i < 799; i++ {
        go say("elk" + strconv.Itoa(i))
    }
	say("elk1000")
}