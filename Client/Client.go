package main

import (
	"coen317/Merge"
	"coen317/common"
	"encoding/gob"
	"flag"
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
	Merge.Sorter(file.Name())

	for i := uint(1); i <= uint(math.Log2(float64(len(addresses)))); i++ {

		// if id mod 2^i != 0, send data to the next host
		if id%(1<<i) != 0 {
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
				fmt.Printf("Sending to %v\n", addresses[id-i]+":"+strconv.Itoa(common.CLIENT_PORT_BASE+int(id-i)))
				common.SendData(file, gob.NewEncoder(conn))
				fmt.Printf("\n")
			}()

			// once we send our data to another host, we're done
			return
		}

		// otherwise, receive data from a host and merge it
		// use a self-invoking function literal so we can defer
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

			// get file size before receiving
			stat, err := file.Stat()
			if err != nil {
				panic(err)
			}
			size1 := stat.Size()

			// receive
			fmt.Printf("Receiving on port %v\n", strconv.Itoa(common.CLIENT_PORT_BASE+int(id)))
			common.RecvData(gob.NewDecoder(conn), file)
			fmt.Printf("\n")

			// get file size after receiving, calculate difference -> size of 2nd half
			stat, err = file.Stat()
			if err != nil {
				panic(err)
			}
			size2 := stat.Size() - size1

			if _, err := file.Seek(0, io.SeekStart); err != nil {
				panic(err)
			}
			file2, err := os.Open(file.Name())
			if err != nil {
				panic(err)
			}
			defer common.Close(file2)
			if _, err := file.Seek(size1, io.SeekStart); err != nil {
				panic(err)
			}
			file3, err := os.OpenFile(file.Name(), os.O_WRONLY, 0600)
			if err != nil {
				panic(err)
			}
			defer common.Close(file3)

			// merge what we have with what we just received
			fmt.Printf("Merging...\tsize1: %v\tsize2: %v\n", size1, size2)
			Merge.Merge(file, file2, size1, size2, file3)
		}()
		fmt.Printf("\n")
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
	fmt.Printf("Sending to controller\n")
	common.SendData(file, gob.NewEncoder(conn))
	fmt.Printf("\n")
}

func main() {
	// set up, parse, and validate args
	var argControllerAddr string
	flag.StringVar(&argControllerAddr, "controller", "", "controller address")
	flag.Parse()
	if argControllerAddr == "" {
		fmt.Printf("Usage: %v -controller <controller address>\n", os.Args[0])
	}

	// connect to the controller
	conn, err := net.Dial("tcp", argControllerAddr)
	if err != nil {
		panic(err)
	}
	decoder := gob.NewDecoder(conn)

	// receive info from controller
	id, addresses, size := getInfo(decoder)
	_ = size

	// create a file with a random name for temp storage
	// defer closing and removing it
	file := makeTempFile()
	defer common.CloseRemove(file)

	// receive data from controller into file
	fmt.Printf("Receiving from controller...\n")
	common.RecvData(decoder, file)
	fmt.Printf("\n")

	// do everything else
	clientRoutine(file, id, addresses)
}
