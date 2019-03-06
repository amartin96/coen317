package main

import (
	"coen317/common"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func Print(file io.Reader) {
	for {
		var x int32
		if err := binary.Read(file, binary.BigEndian, &x); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", x)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %v <file>\n", os.Args[0])
		return
	}

	file, err := os.Open(os.Args[1])
	common.PanicOnError(err)
	defer common.Close(file)
	Print(file)
}
