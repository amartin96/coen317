package common

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
)

const CONTROLLER_PORT = 12345
const CLIENT_PORT_BASE = 12346
const BUFSIZE = 2

type ClientInfo struct {
	Id        uint
	Addresses []string
	Size      int64
}

func Close(closer io.Closer) {
	if err := closer.Close(); err != nil {
		panic(err)
	}
}

func CloseRemove(file *os.File) {
	Close(file)
	if err := os.Remove(file.Name()); err != nil {
		panic(err)
	}
}

func RecvData(conn io.Reader, file io.Writer) {
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
