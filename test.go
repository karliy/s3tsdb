package main

import (
  "log"
  "fmt"
  "strconv"
  "github.com/boltdb/bolt"
)
func Use(vals ...interface{}) {
    for _, val := range vals {
        _ = val
    }
}
func main() {
  for a := 0; a < 1; a++ {
    db, err := bolt.Open("/home/coding/tmp/"+strconv.Itoa(a)+"my.db", 0600, nil)
    if err != nil {
      log.Fatal(err)
    }
    
    //db.Update(func(tx *bolt.Tx) error {
    //  b, err := tx.CreateBucket([]byte("MyBucket"))
    //  if err != nil {
    //    return fmt.Errorf("create bucket: %s", err)
    //  }
    //  Use(b)
    //  return nil
    //})
    db.Update(func(tx *bolt.Tx) error {
      b := tx.Bucket([]byte("MyBucket"))
      //for c := 0; c < 1000000; c++ {
      //  err := b.Put([]byte("answer"+strconv.Itoa(c)), []byte("42"))
      //  Use(err)
      //}
      v := b.Get([]byte("answer980104"))
      fmt.Printf("The answer is: %s\n", v)
      //Use(v)
      return err
    })
    
    
    defer db.Close()
  }
}