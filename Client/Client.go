package main

import (
	"encoding/gob"
	"fmt"
	"net"
)

func main() {
	socket, err := net.Dial("tcp", "localhost:12345")
	if err != nil {
		panic(err)
	}
	decoder := gob.NewDecoder(socket)
	var addresses []string
	err = decoder.Decode(&addresses)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", addresses)
}
