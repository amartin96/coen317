package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"time"
)

const maxMemory = 32

// sorter is used to sort a file of integers on machines with memory equal to maxMemory
func sorter(filename string) {
	f1, _:= os.OpenFile(filename, os.O_RDWR, 0600)
	f2, _ := os.OpenFile(filename, os.O_RDWR, 0600)
	fInfo, _ := f1.Stat()
	fileSize := fInfo.Size()

	// do quicksort on blocks of the maximum memory size
	buffer := make([]int32, maxMemory/4)
	for i := int64(0); i < (fileSize/int64(maxMemory)); i++ {

		// get current position of file pointer
		f1Pos, _ := f1.Seek(0,1)

		// read in a block at a time
		_ = binary.Read(f1, binary.BigEndian, buffer)

		// sort the block
		sort.SliceStable(buffer, func(t, p int) bool { return buffer[t] < buffer[p] })

		// seek back so we can re-write the block
		_, err := f1.Seek(f1Pos, 0)
		if err != nil {
			panic(err)
		}
		// write the block back
		_ = binary.Write(f1, binary.BigEndian, buffer)
	}

	// do the merging
	var blockCount int64
	var curBlock int64
	var blockSize int64 = maxMemory
	for blockSize < fileSize {
		blockCount = fileSize/blockSize
		curBlock = 0
		_,_ = f1.Seek(0,0)
		_,_ = f2.Seek(0,0)
		for curBlock < blockCount {
			limitR1 := io.LimitReader(f1, blockSize)
			limitR2 := io.LimitReader(f2, blockSize)
			merge(limitR1, limitR2)
			_,_ = f1.Seek(blockSize,1)
			_,_ = f2.Seek(blockSize, 1)
			blockSize *= 2
			curBlock++
		}
	}
}


// merging function
func merge(r1 io.Reader, r2 io.Reader) {
	// buffers for reading in the binary
	bufferSize := maxMemory/8
	buffer1 := make([]int32, bufferSize)
	buffer2 := make([]int32, bufferSize)
	sorted := make([]int32, maxMemory/4)
	tempFile, _ := ioutil.TempFile("", "temp")

	// indices for buffers
	buffer1Index := 0
	buffer2Index := 0
	sortedIndex := 0

	// main merging algorithm
	for {
		// load new data if necessary
		if buffer1Index == bufferSize {
			err := binary.Read(r1, binary.LittleEndian, buffer1)
			if err != nil {
				break;
			}
			buffer1Index = 0
		} else if buffer2Index == bufferSize {
			err := binary.Read(r2, binary.LittleEndian, buffer2)
			if err != nil {
				break
			}
			buffer2Index = 0
		}

		// comparison and merging
		if buffer1[buffer1Index] <= buffer2[buffer2Index] {
			sorted[sortedIndex] = buffer1[buffer1Index]
			buffer1Index++
			sortedIndex++
		} else {
			sorted[sortedIndex] = buffer2[buffer2Index]
			buffer2Index++
			sortedIndex++
		}

		// write sorted block to file
		if sortedIndex == 8 {
			_ = binary.Write(tempFile, binary.BigEndian, sorted)
			sortedIndex = 0
		}
	}

	// account for remaining block 1 elements
	for {
		// load new data if necessary
		if buffer1Index == bufferSize {
			err := binary.Read(r1, binary.LittleEndian, buffer1)
			if err == nil {
				break
			}
			buffer1Index = 0
		}

		sorted[sortedIndex] = buffer1[buffer1Index]
		buffer1Index++
		sortedIndex++

		// write sorted block to file
		if sortedIndex == 8 {
			_ = binary.Write(tempFile, binary.BigEndian, sorted)
			sortedIndex = 0
		}
	}

	// account for remaining block 2 elements
	for {
		// load new data if necessary
		if buffer2Index == bufferSize {
			err := binary.Read(r2, binary.LittleEndian, buffer2)
			if err != nil {
				break
			}
			buffer2Index = 0
		}
		sorted[sortedIndex] = buffer2[buffer2Index]
		buffer2Index++
		sortedIndex++

		// write sorted block to file
		if sortedIndex == 8 {
			_ = binary.Write(tempFile, binary.BigEndian, sorted)
			sortedIndex = 0
		}
	}

	// move temporary file back to the beginning
	_,_ = tempFile.Seek(0,0)

	_ := tempFile.Close()
}

// writes blockCount blocks of maxMemory worth of random int32s
func randomIntFile(blockCount int, filename string) {
	// seed random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	// writing memory for testing
	f1, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0600)
	currentData := make([]int32, maxMemory/4)

	// runs the amount of chunks you have to write sorted data to file
	for i := 0; i < blockCount; i++ {

		// generate a random chunk of data
		for j := 0; j < maxMemory/4; j++ {
			x := int32(rand.Intn(50))
			currentData[j] = x
		}

		// write data to file
		err1 := binary.Write(f1, binary.BigEndian, currentData)
		if err1 != nil {
			panic(err1)
		}
	}
	_ = f1.Close()
}

// prints binary file of ints in blocks of max memory
func printBinaryIntFile(filename string) {
	f1, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	buffer := make([]int32, 8)
	err = binary.Read(f1, binary.BigEndian, buffer)
	for  err == nil {
		fmt.Print(buffer)
		err = binary.Read(f1, binary.BigEndian, buffer)
	}
	err = f1.Close()
	if err != nil {
		panic(err)
	}
}


func main() {
	//randomIntFile(4,"ints.txt")
	//sorter("ints.txt")
	//printBinaryIntFile("ints.txt")
}
