package main

import (
	"coen317/common"
	"encoding/gob"
	"flag"
	"fmt"
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

func acceptClients(server net.Listener, numClients int) ([]net.Conn, []string) {
	clients := make([]net.Conn, numClients)
	addresses := make([]string, numClients)

	for i := 0; i < numClients; i++ {
		var err error
		clients[i], err = server.Accept()
		if err != nil {
			panic(err)
		}
		addresses[i] = clients[i].RemoteAddr().String()
		fmt.Printf("Client %v connected\n", addresses[i])
	}

	return clients, addresses
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

	// open the file and get its size
	file, size := getFile(*argFileName)
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	fmt.Printf("%v size: %v\n", file.Name(), size)

	// start listening, defer closing the listen socket
	server, err := net.Listen("tcp", ":"+*argPort)
	if err != nil {
		panic(err)
	}
	defer func() {
		err = server.Close()
		if err != nil {
			panic(err)
		}
	}()

	// accept connections from all clients
	clients, addresses := acceptClients(server, *argNumClients)

	// for each client
	// - defer closing the connection
	// - send client info
	// - send a piece of the file
	for i := 0; i < len(clients); i++ {
		// defer closing the connection
		defer func(client net.Conn) {
			err = client.Close()
			if err != nil {
				panic(err)
			}
		}(clients[i])

		// send client info TODO compute the size correctly
		encoder := gob.NewEncoder(clients[i])
		if err := encoder.Encode(common.ClientInfo{Id: i, Addresses: addresses, Size: size}); err != nil {
			panic(err)
		}

		// send part of file TODO this sends the whole file at once right now
		buffer := make([]byte, size)
		_, _ = file.Read(buffer)
		fmt.Printf("%v\n", buffer)
		if err := encoder.Encode(buffer); err != nil {
			panic(err)
		}
	}
}
