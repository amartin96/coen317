package main

import (
	"coen317/Merge"
	"coen317/common"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
)

func getFile(name string) (*os.File, int64) {
	path, err := filepath.Abs(name)
	if err != nil {
		panic(err)
	}

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	return file, stat.Size()
}

func acceptClients(server net.Listener, numClients int) ([]net.Conn, []net.IP) {
	clients := make([]net.Conn, numClients)
	addresses := make([]net.IP, numClients)

	for i := 0; i < numClients; i++ {
		var err error
		clients[i], err = server.Accept()
		common.PanicOnError(err)
		addresses[i] = clients[i].RemoteAddr().(*net.TCPAddr).IP
		fmt.Printf("Client %v connected\n", clients[i].RemoteAddr().String())
	}

	fmt.Printf("\n")

	return clients, addresses
}

func sendToClient(file io.Reader, conn io.Writer, info common.ClientInfo, size int64) {
	encoder := gob.NewEncoder(conn)
	common.PanicOnError(encoder.Encode(info))
	reader := io.LimitReader(file, size)
	common.SendData(reader, encoder)
	fmt.Printf("\n")
}

func main() {
	// parse and validate command line arguments
	var args struct {
		Port       string
		FileName   string
		NumClients int
	}
	flag.StringVar(&args.Port, "port", "", "listen port")
	flag.StringVar(&args.FileName, "file", "", "file to be sorted")
	flag.IntVar(&args.NumClients, "clients", 0, "# clients")
	flag.Parse()
	if args.Port == "" || args.FileName == "" || args.NumClients == 0 {
		fmt.Printf("Usage: %v -port <port> -file <file> -clients <clients>\n", os.Args[0])
		return
	}
	if math.Ceil(float64(args.NumClients)) != math.Floor(float64(args.NumClients)) {
		fmt.Printf("Error: # clients must be a power of 2!\n")
		return
	}

	// open the file and get its size
	file, size := getFile(args.FileName)
	defer common.Close(file)
	chunkSize := size / int64(args.NumClients) / 4 * 4 // if this doesn't divide cleanly, then the last client has extra work
	fmt.Printf("%v size: %v clientDataSize: %v\n", file.Name(), size, chunkSize)

	// start listening, defer closing the listen socket
	listener, err := net.Listen("tcp", ":"+args.Port)
	common.PanicOnError(err)
	defer common.Close(listener)

	// accept connections from all clients
	clients, addresses := acceptClients(listener, args.NumClients)

	// send data to each client
	for i, client := range clients {
		// use a self-evaluating function literal so we can defer stuff
		func() {
			// defer closing the connection to the client
			defer common.Close(client)

			clientDataSize := chunkSize
			if i == len(clients)-1 {
				clientDataSize = size - chunkSize*int64(len(clients)-1)
			}
			fmt.Printf("clientDataSize %v: %v\n", i, clientDataSize)
			sendToClient(file, client, common.ClientInfo{Id: uint(i), Addresses: addresses}, clientDataSize)
		}()
	}

	// receive the sorted data back from client 0
	conn, err := listener.Accept()
	common.PanicOnError(err)
	defer common.Close(conn)
	outfile, err := os.Create("out")
	common.PanicOnError(err)
	defer common.Close(outfile)
	common.RecvData(gob.NewDecoder(conn), outfile)
	fmt.Printf("\n")

	fmt.Printf("Sorted file:\n")
	Merge.PrintBinaryIntFile(outfile.Name())
}
