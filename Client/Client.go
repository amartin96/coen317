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

// receive and decode a common.ClientInfo struct from the controller
func getInfo(decoder *gob.Decoder) (uint, []string, int64) {
	var info common.ClientInfo
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
func getData(decoder *gob.Decoder, file io.Writer, size int64) {
	fmt.Printf("size: %v\n", size)

	var buffer []byte
	for size > 0 {
		if err := decoder.Decode(&buffer); err != nil {
			panic(err)
		}

		fmt.Printf("received %v\n", buffer)

		if _, err := file.Write(buffer); err != nil {
			panic(err)
		}
		size -= int64(len(buffer))
	}
}

// send the contents of file to conn
func sendData(file io.Reader, conn io.Writer) {
	encoder := gob.NewEncoder(conn)
	buffer := make([]byte, common.BUFSIZE)

	for {
		if _, err := file.Read(buffer); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if err := encoder.Encode(buffer); err != nil {
			panic(err)
		}
	}
}

// this differs from getData because the connection ends after the file is transmitted
func recvData(conn io.Reader, file io.Writer) error {
	var buffer []byte
	decoder := gob.NewDecoder(conn)

	for {
		if err := decoder.Decode(&buffer); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if _, err := file.Write(buffer); err != nil {
			return err
		}
	}

	return nil
}

// While not done:
//	- sort my data
//	- am I sending or receiving?
// 		sending:
//			- send data
//			- done
//		receiving:
//			- receive data
//			- keep going
func clientRoutine(file io.ReadWriter, id uint, addresses []string) {
	// TODO sort

	for i := uint(1); ; i++ {

		// if id mod 2^i != 0, send data to the next host
		if id%(1<<i) != 0 {
			conn, err := net.Dial("tcp", addresses[id-i])
			if err != nil {
				panic(err)
			}
			defer common.Close(conn) // this defer is ok, we return during this iteration of the loop
			sendData(file, conn)
			return
		}

		// otherwise, receive data from a host and merge it
		// lots of ugly error handling because deferring here would be bad
		server, err := net.Listen("tcp", ":"+common.PORT)
		if err != nil {
			panic(err)
		}

		conn, err := server.Accept()
		if err != nil {
			common.Close(server)
			panic(err)
		}
		if err := recvData(conn, file); err != nil {
			common.Close(conn)
			common.Close(server)
			panic(err)
		}
		common.Close(conn)
		common.Close(server)
		// TODO merge
	}
}

func main() {
	// connect to the controller
	conn, err := net.Dial("tcp", "localhost:"+common.PORT)
	if err != nil {
		panic(err)
	}
	decoder := gob.NewDecoder(conn)

	// receive info from controller
	id, addresses, size := getInfo(decoder)
	_, _ = id, addresses // TODO remove once referenced

	// create a file with a random name for temp storage
	// defer closing and removing it
	file := makeTempFile()
	defer common.CloseRemove(file)

	// receive data from controller into file
	getData(decoder, file, size)

	// do everything else
	//clientRoutine(file, id, addresses)
}
