package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

func main() {
	// set up and parse command line arguments
	argPort := flag.String("port", "", "listen port")
	argFileName := flag.String("file", "", "file to be sorted")
	argNumClients := flag.Int("clients", 0, "# clients")
	flag.Parse()
	if *argFileName == "" || *argNumClients == 0 || *argPort == "" {
		// TODO these are required arguments
		// TODO actually probably use a different args package cuz the builtin one sucks
		fmt.Printf("Usage: %v -port <port> -file <file> -clients <clients>\n", os.Args[0])
		return
	}

	// open the data file, get its size
	relPath, err := filepath.Abs(*argFileName)
	if err != nil {
		panic(err)
	}
	file, err := os.Open(relPath)
	if err != nil {
		panic(err)
	}
	defer func() {
		err = file.Close()
		if err != nil {
			panic(err)
		}
	}()
	fileinfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v size: %v\n", *argFileName, fileinfo.Size())

	// listen for and accept connections from all of the clients
	// note their addresses
	server, err := net.Listen("tcp", ":"+*argPort)
	if err != nil {
		panic(err)
	}
	clients := make([]net.Conn, *argNumClients)
	addresses := make([]string, *argNumClients)
	for i := 0; i < len(clients); i++ {
		clients[i], err = server.Accept()
		if err != nil {
			panic(err)
		}
		addresses[i] = clients[i].RemoteAddr().String()
		fmt.Printf("%v\n", addresses[i])
	}

	// encode the array of addresses
	buffer := new(bytes.Buffer)
	err = gob.NewEncoder(buffer).Encode(addresses)
	if err != nil {
		panic(err)
	}

	// send the array of client addresses to each client
	for i := 0; i < len(clients); i++ {
		_, err := clients[i].Write(buffer.Bytes())
		if err != nil {
			panic(err)
		}
	}

	// clean up
	for _, client := range clients {
		err = client.Close()
		if err != nil {
			panic(err)
		}
	}
}
