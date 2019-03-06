package main

import (
	"coen317/common"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"reflect"
)

func getMap(file io.Reader) map[int32]int {
	retval := make(map[int32]int)
	for {
		var x int32
		if err := binary.Read(file, binary.BigEndian, &x); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		retval[x]++
	}
	return retval
}

func Validate(file1 io.Reader, file2 io.Reader) bool {
	return reflect.DeepEqual(getMap(file1), getMap(file2))
}

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %v file1 file2\n", os.Args[0])
		return
	}

	file1, err := os.Open(os.Args[1])
	common.PanicOnError(err)
	defer common.Close(file1)
	file2, err := os.Open(os.Args[2])
	common.PanicOnError(err)
	defer common.Close(file2)
	fmt.Printf("%v\n", Validate(file1, file2))
}
