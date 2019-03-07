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

// check that a file is sorted in ascending order
func isSorted(file io.Reader) bool {
	var x, prev int32
	common.PanicOnError(binary.Read(file, binary.BigEndian, &prev))

	for {
		if err := binary.Read(file, binary.BigEndian, &x); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if x < prev {
			return false
		}

		prev = x
	}

	return true
}

// check if two files contain the same integers (in any order)
func Validate(file1 io.Reader, file2 io.Reader) bool {
	return reflect.DeepEqual(getMap(file1), getMap(file2))
}

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %v unsorted_file sorted_file\n", os.Args[0])
		return
	}

	file1, err := os.Open(os.Args[1])
	common.PanicOnError(err)
	defer common.Close(file1)
	file2, err := os.Open(os.Args[2])
	common.PanicOnError(err)
	defer common.Close(file2)

	result := Validate(file1, file2)
	_, err = file2.Seek(0, io.SeekStart)
	common.PanicOnError(err)
	result = result && isSorted(file2)

	fmt.Printf("%v\n", result)
}
