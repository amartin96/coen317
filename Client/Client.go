package main

import (
	"coen317/common"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
)

const TEMPFILEPREFIX = "coen317"

// establish a TCP connection with the controller
func connect(address string) net.Conn {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		panic(err)
	}
	return conn
}

// receive and decode a common.ClientInfo struct from the controller
func getInfo(conn io.Reader) (int, []string, int64) {
	var info common.ClientInfo
	decoder := gob.NewDecoder(conn)
	if err := decoder.Decode(&info); err != nil {
		panic(err)
	}
	return info.Id, info.Addresses, info.Size
}

func makeTempFile() *os.File {
	file, err := ioutil.TempFile("", TEMPFILEPREFIX)
	if err != nil {
		panic(err)
	}
	return file
}

// receive the data to be sorted from the controller and write it to a file
func getData(conn io.Reader, file io.Writer, size int64) {
	var buffer []byte
	decoder := gob.NewDecoder(conn)
	for size > 0 {
		if err := decoder.Decode(&buffer); err != nil {
			panic(err)
		}
		if _, err := file.Write(buffer); err != nil {
			panic(err)
		}
		size -= int64(len(buffer))
	}
}

func main() {
	// connect to the controller
	conn := connect("localhost:12345")

	id, addresses, size := getInfo(conn)
	_ = id        // TODO remove once referenced
	_ = addresses // TODO remove once referenced
	_ = size      // TODO remove once referenced

	// create a file with a random name for temp storage
	// defer closing and removing it
	file := makeTempFile()
	fmt.Printf("Created temp file %v\n", file.Name())
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
		if err := os.Remove(file.Name()); err != nil {
			panic(err)
		}
		fmt.Printf("Removed temp file %v\n", file.Name())
	}()

	getData(conn, file, size)
}
