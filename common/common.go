package common

import (
	"encoding/gob"
	"io"
	"net"
	"os"
)

type ClientInfo struct {
	Id        int
	Addresses []net.IP
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

func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func RecvData(decoder *gob.Decoder, file io.Writer) {
	var buffer []byte

	for {
		if err := decoder.Decode(&buffer); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if _, err := file.Write(buffer); err != nil {
			panic(err)
		}
	}
}

func SendData(file io.Reader, encoder *gob.Encoder, bufsize int) {
	buffer := make([]byte, bufsize)

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if err := encoder.Encode(buffer[:n]); err != nil {
			panic(err)
		}
	}
}
