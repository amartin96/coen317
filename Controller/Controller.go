package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/amartin96/coen317/common"
	"io"
	"math"
	"net"
	"os"
	"time"
)

var args struct {
	Port        int
	InFileName  string
	OutFileName string
	NumClients  int
	Bufsize     int
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

func sendToClientAndClose(reader io.Reader, writer io.WriteCloser, info common.ClientInfo) {
	defer common.Close(writer)
	encoder := gob.NewEncoder(writer)
	common.PanicOnError(encoder.Encode(info))
	common.SendData(reader, encoder, args.Bufsize)
}

func controllerRoutine(infile io.Reader, outfile io.Writer, sizePerClient int64) {
	// listen on the specified port
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{Port: args.Port})
	common.PanicOnError(err)
	defer common.Close(listener)

	// accept connections from all clients
	clients, addresses := acceptClients(listener, args.NumClients)

	// start timing
	timestamp := time.Now()

	// send sizePerClient bytes of the file to all but the last client
	for i, client := range clients[:len(clients)-1] {
		sendToClientAndClose(io.LimitReader(infile, sizePerClient), client, common.ClientInfo{Id: i, Addresses: addresses})
	}

	// send the rest of the file to the last client
	sendToClientAndClose(infile, clients[len(clients)-1], common.ClientInfo{Id: len(clients) - 1, Addresses: addresses})

	// receive sorted data from client 0
	conn, err := listener.Accept()
	common.PanicOnError(err)
	defer common.Close(conn)
	common.RecvData(gob.NewDecoder(conn), outfile)

	// finish timing
	fmt.Printf("Time elapsed: %v\n", time.Since(timestamp))
}

func main() {
	// parse and validate command line arguments
	flag.IntVar(&args.Port, "port", 0, "listen port")
	flag.StringVar(&args.InFileName, "in", "", "file to be sorted")
	flag.StringVar(&args.OutFileName, "out", "", "sorted results")
	flag.IntVar(&args.NumClients, "clients", 0, "# clients")
	flag.IntVar(&args.Bufsize, "buffer", 0, "buffer size")
	flag.Parse()
	if args.Port == 0 || args.InFileName == "" || args.OutFileName == "" || args.NumClients == 0 || args.Bufsize == 0 {
		fmt.Printf("Usage: %v -port <port> -in <infile> -out <outfile> -clients <clients> -buffer <buffer size>\n", os.Args[0])
		return
	}
	if math.Ceil(float64(args.NumClients)) != math.Floor(float64(args.NumClients)) {
		fmt.Printf("Error: # clients must be a power of 2!\n")
		return
	}
	if args.Bufsize%4 != 0 {
		fmt.Printf("Error: buffer size must be a multiple of 4 bytes!\n")
		return
	}

	// open the file and get its size
	file, err := os.Open(args.InFileName)
	common.PanicOnError(err)
	defer common.Close(file)
	stat, err := file.Stat()
	common.PanicOnError(err)
	sizePerClient := stat.Size() / int64(args.NumClients) / 4 * 4 // if this doesn't divide cleanly, then the last client has extra work

	// open the output file
	outfile, err := os.Create(args.OutFileName)
	common.PanicOnError(err)

	controllerRoutine(file, outfile, sizePerClient)
}
