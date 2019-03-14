package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/amartin96/coen317/Merge"
	"github.com/amartin96/coen317/common"
	"io"
	"io/ioutil"
	"net"
	"os"
	"time"
)

const TEMPFILEPREFIX = "coen317"

var args struct {
	BasePort       int
	ControllerAddr string
	Bufsize        int
}

// computes: y = floor(log2(x))
func intlog2(x int) int {
	y := 0
	for {
		x = x >> 1
		if x == 0 {
			break
		}
		y++
	}
	return y
}

func sendToClient(file io.Reader, address *net.TCPAddr) {
	// connect to the receiving client
	// retry until successful
	var conn net.Conn
	for {
		var err error
		conn, err = net.DialTCP("tcp", nil, address)
		if err == nil {
			break
		}
		fmt.Println(err)
		fmt.Printf("Failed to connect to %v. Retrying...\n", address.String())
		time.Sleep(time.Millisecond)
	}
	defer common.Close(conn)
	fmt.Printf("Sending to %v\n", address.String())
	common.SendData(file, gob.NewEncoder(conn), args.Bufsize)
}

func recvFromClient(address *net.TCPAddr) *os.File {
	// listen
	server, err := net.ListenTCP("tcp", address)
	common.PanicOnError(err)
	defer common.Close(server)

	// accept
	conn, err := server.Accept()
	common.PanicOnError(err)
	defer common.Close(conn)

	// create a file to receive into
	file, err := ioutil.TempFile("", TEMPFILEPREFIX)
	common.PanicOnError(err)

	fmt.Printf("Receiving from %v\n", conn.RemoteAddr().String())
	common.RecvData(gob.NewDecoder(conn), file)

	_, err = file.Seek(0, io.SeekStart)
	common.PanicOnError(err)

	return file
}

func merge(file1 *os.File, file2 *os.File) {
	stat, err := file1.Stat()
	common.PanicOnError(err)
	size1 := stat.Size()
	stat, err = file2.Stat()
	common.PanicOnError(err)
	size2 := stat.Size()
	file3, err := os.OpenFile(file1.Name(), os.O_WRONLY, 0600)
	common.PanicOnError(err)
	Merge.Merge(file1, file2, size1, size2, file3, args.Bufsize)
	_, err = file1.Seek(0, io.SeekStart)
	common.PanicOnError(err)
}

func clientRoutine(file *os.File, id int, addresses []net.IP) {
	fmt.Printf("Sorting\n")
	Merge.Sorter(file.Name(), args.Bufsize)

	for i := 1; i <= intlog2(len(addresses)); i++ {
		// if id mod 2^i != 0, send data to the next host
		if id%(1<<uint(i)) != 0 {
			sendToClient(file, &net.TCPAddr{IP: addresses[id-i], Port: args.BasePort + id - i})
			return
		}

		// otherwise, receive data from a host and merge it
		file2 := recvFromClient(&net.TCPAddr{Port: args.BasePort + id})
		merge(file, file2)
		common.Close(file2) // TODO defer this somehow

		_, err := file.Seek(0, io.SeekStart)
		common.PanicOnError(err)
	}

	// if execution makes it here, we are client 0 and everything has been merged
	// send the complete results back to the controller
	conn, err := net.Dial("tcp", args.ControllerAddr)
	common.PanicOnError(err)
	_, err = file.Seek(0, io.SeekStart)
	common.PanicOnError(err)
	fmt.Printf("Sending to controller\n")
	common.SendData(file, gob.NewEncoder(conn), args.Bufsize)
}

func main() {
	// set up, parse, and validate args
	flag.IntVar(&args.BasePort, "base_port", 0, "client port = base port + client id")
	flag.StringVar(&args.ControllerAddr, "controller", "", "controller address")
	flag.IntVar(&args.Bufsize, "buffer", 0, "buffer size")
	flag.Parse()
	if args.BasePort == 0 || args.ControllerAddr == "" || args.Bufsize == 0 {
		fmt.Printf("Usage: %v -base_port <base port> -controller <controller address> -buffer <buffer size>\n", os.Args[0])
		return
	}

	// connect to the controller
	conn, err := net.Dial("tcp", args.ControllerAddr)
	common.PanicOnError(err)
	decoder := gob.NewDecoder(conn)

	// receive info from controller
	var info common.ClientInfo
	common.PanicOnError(decoder.Decode(&info))

	// create a file with a random name for temp storage
	// defer closing and removing it
	file, err := ioutil.TempFile("", TEMPFILEPREFIX)
	common.PanicOnError(err)
	defer common.CloseRemove(file)

	// receive data from controller into file
	fmt.Printf("Receiving from controller\n")
	common.RecvData(decoder, file)
	_, err = file.Seek(0, io.SeekStart)
	common.PanicOnError(err)

	// do everything else
	clientRoutine(file, info.Id, info.Addresses)
}
