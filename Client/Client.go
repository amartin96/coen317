package main

import (
	"coen317/common"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"os"
	"strconv"
	"time"
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
// TODO if the controller closes the connection, we can use EOF and eliminate the need to know the size beforehand
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
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		fmt.Printf("sending %v\n", buffer[:n])

		if err := encoder.Encode(buffer[:n]); err != nil {
			panic(err)
		}
	}
}

// this differs from getData because the connection ends after the file is transmitted
func recvData(conn io.Reader, file io.Writer) {
	var buffer []byte
	decoder := gob.NewDecoder(conn)

	for {
		if err := decoder.Decode(&buffer); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		fmt.Printf("received %v\n", buffer)

		if _, err := file.Write(buffer); err != nil {
			panic(err)
		}
	}
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
func clientRoutine(file *os.File, id uint, addresses []string) {
	// TODO sort

	for i := uint(1); i <= uint(math.Log2(float64(len(addresses)))); i++ {

		// if id mod 2^i != 0, send data to the next host
		if id%(1<<i) != 0 {
			fmt.Printf("sending to %v\n", addresses[id-i]+":"+strconv.Itoa(common.CLIENT_PORT_BASE+int(id-i)))
			time.Sleep(time.Second) // TODO figure something else out

			// use a self-invoking function literal so we can defer
			func() {
				conn, err := net.Dial("tcp", addresses[id-i]+":"+strconv.Itoa(common.CLIENT_PORT_BASE+int(id-i)))
				if err != nil {
					panic(err)
				}
				defer common.Close(conn)
				if _, err := file.Seek(0, io.SeekStart); err != nil {
					panic(err)
				}
				sendData(file, conn)
			}()

			// once we send our data to another host, we're done
			return
		}

		// otherwise, receive data from a host and merge it
		// use a self-invoking function literal so we can defer
		fmt.Printf("receiving on port %v\n", strconv.Itoa(common.CLIENT_PORT_BASE+int(id)))
		func() {
			server, err := net.Listen("tcp", ":"+strconv.Itoa(common.CLIENT_PORT_BASE+int(id)))
			if err != nil {
				panic(err)
			}
			defer common.Close(server)

			conn, err := server.Accept()
			if err != nil {
				panic(err)
			}
			defer common.Close(conn)

			if _, err := file.Seek(0, io.SeekEnd); err != nil {
				panic(err)
			}

			recvData(conn, file)
			// TODO merge
		}()
	}

	// if execution makes it here, we are client 0 and everything has been merged
	// send the complete results back to the controller
	conn, err := net.Dial("tcp", "localhost:12345") // TODO get the actual controller address
	if err != nil {
		panic(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}
	sendData(file, conn)
}

func main() {
	// connect to the controller
	conn, err := net.Dial("tcp", "localhost:"+strconv.Itoa(common.CONTROLLER_PORT))
	if err != nil {
		panic(err)
	}
	decoder := gob.NewDecoder(conn)

	// receive info from controller
	id, addresses, size := getInfo(decoder)

	// create a file with a random name for temp storage
	// defer closing and removing it
	file := makeTempFile()
	defer common.CloseRemove(file)

	// receive data from controller into file
	getData(decoder, file, size)

	// do everything else
	clientRoutine(file, id, addresses)
}
