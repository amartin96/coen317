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
	"strings"
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

func acceptClients(server net.Listener, numClients int) ([]net.Conn, []string) {
	clients := make([]net.Conn, numClients)
	addresses := make([]string, numClients)

	for i := 0; i < numClients; i++ {
		var err error
		clients[i], err = server.Accept()
		if err != nil {
			panic(err)
		}
		addresses[i] = strings.Split(clients[i].RemoteAddr().String(), ":")[0]
		fmt.Printf("Client %v connected\n", clients[i].RemoteAddr().String())
	}

	fmt.Printf("\n")

	return clients, addresses
}

func sendToClient(file io.Reader, conn io.Writer, info common.ClientInfo) {
	encoder := gob.NewEncoder(conn)
	if err := encoder.Encode(info); err != nil {
		panic(err)
	}

	reader := io.LimitReader(file, info.Size)
	common.SendData(reader, encoder)
	fmt.Printf("\n")
}

func main() {
	// set up and parse command line arguments
	argPort := flag.String("port", "", "listen port")
	argFileName := flag.String("file", "", "file to be sorted")
	argNumClients := flag.Int("clients", 0, "# clients")
	flag.Parse()
	if *argFileName == "" || *argNumClients == 0 || *argPort == "" {
		fmt.Printf("Usage: %v -port <port> -file <file> -clients <clients>\n", os.Args[0])
		return
	}
	if math.Ceil(float64(*argNumClients)) != math.Floor(float64(*argNumClients)) {
		fmt.Printf("Error: # clients must be a power of 2!\n")
		return
	}

	// TODO just for testing
	Merge.RandomIntFile(3, *argFileName, 255)

	// open the file and get its size
	file, size := getFile(*argFileName)
	defer common.Close(file)
	chunkSize := size / int64(*argNumClients) / 4 * 4 // if this doesn't divide cleanly, then the last client has extra work
	fmt.Printf("%v size: %v chunkSize: %v\n", file.Name(), size, chunkSize)

	// start listening, defer closing the listen socket
	server, err := net.Listen("tcp", ":"+*argPort)
	if err != nil {
		panic(err)
	}
	defer common.Close(server)

	// accept connections from all clients
	clients, addresses := acceptClients(server, *argNumClients)

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
			sendToClient(file, client, common.ClientInfo{Id: uint(i), Addresses: addresses, Size: clientDataSize})
		}()
	}

	// receive the sorted data back from client 0
	conn, err := server.Accept()
	if err != nil {
		panic(err)
	}
	defer common.Close(conn)
	outfile, err := os.Create("out")
	if err != nil {
		panic(err)
	}
	defer common.Close(outfile)
	common.RecvData(gob.NewDecoder(conn), outfile)
	fmt.Printf("\n")

	fmt.Printf("Sorted file:\n")
	Merge.PrintBinaryIntFile(outfile.Name())
}
