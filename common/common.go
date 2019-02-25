package common

import (
	"io"
	"os"
)

const PORT = "12345"
const BUFSIZE = 1024

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
