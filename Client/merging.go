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

// sort a file of integers on machines with maximum memory equal to maxMemory
func Sorter(filename string) {
	f1, err := os.OpenFile(filename, os.O_RDWR, 0600)
	checkError(err)
	f2, err := os.OpenFile(filename, os.O_RDWR, 0600)
	checkError(err)
	f3, err := os.OpenFile(filename, os.O_RDWR, 0600)
	checkError(err)
	fInfo, err := f1.Stat()
	checkError(err)
	fileSize := fInfo.Size()

	// perform quicksort on blocks of the maximum memory size
	buffer := make([]int32, maxMemory/4)
	for i := int64(0); i < (fileSize/int64(maxMemory)); i++ {
		// get current position of file pointer
		f1Pos, err := f1.Seek(0,1)
		checkError(err)

		// read in a block at a time
		err = binary.Read(f1, binary.BigEndian, buffer)
		checkError(err)

		// sort the block
		sort.SliceStable(buffer, func(t, p int) bool { return buffer[t] < buffer[p] })

		// seek back so we can re-write the block
		_, err = f1.Seek(f1Pos, 0)
		checkError(err)

		// write the block back
		err = binary.Write(f1, binary.BigEndian, buffer)
		checkError(err)
	}

	// do the merging
	var blockCount int64
	var curBlock int64
	var blockSize int64 = maxMemory
	for blockSize < fileSize {
		blockCount = fileSize/blockSize
		curBlock = 0
		_, err = f1.Seek(0,0)
		checkError(err)
		_, err = f2.Seek(blockSize,0)
		checkError(err)
		_, err = f3.Seek(0,0)
		checkError(err)
		for curBlock < blockCount {
			// get limit readers of current blockSize
			limitR1 := io.LimitReader(f1, blockSize)
			limitR2 := io.LimitReader(f2, blockSize)

			// pass limit readers to have content merged together
			Merge(limitR1, limitR2, f3)

			// seek file pointers to be point at next blocks to be merged
			_,err := f1.Seek(blockSize,1)
			checkError(err)
			_,err = f2.Seek(blockSize, 1)
			checkError(err)

			// progress current block to next two blocks to be merged
			curBlock += 2
		}
		// double block size for next merging step
		blockSize *= 2
	}
}


// perform actual merging of blocks
func Merge(r1 io.Reader, r2 io.Reader, f io.Writer) {
	// buffers for reading in the binary
	bufferSize := maxMemory/8
	buffer1 := make([]int32, bufferSize)
	buffer2 := make([]int32, bufferSize)
	sorted := make([]int32, maxMemory/4)
	tempFile, err := ioutil.TempFile("", "temp")
	checkError(err)

	// indices for buffers
	buffer1Index := 0
	buffer2Index := 0
	sortedIndex := 0

	err = binary.Read(r1, binary.BigEndian, buffer1)
	if err != nil {
		return
	}

	err = binary.Read(r2, binary.BigEndian, buffer2)
	if err != nil {
		return
	}

	// main merging algorithm
	for {
		// load new data if necessary
		if buffer1Index == bufferSize {
			err := binary.Read(r1, binary.BigEndian, buffer1)
			if err != nil {
				break;
			}
			buffer1Index = 0
		} else if buffer2Index == bufferSize {
			err := binary.Read(r2, binary.BigEndian, buffer2)
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
			err = binary.Write(tempFile, binary.BigEndian, sorted)
			checkError(err)
			sortedIndex = 0
		}
	}


	// account for remaining block 1 elements
	for {
		// load new data if necessary
		if buffer1Index == bufferSize {
			err := binary.Read(r1, binary.BigEndian, buffer1)
			if err != nil {
				break
			}
			buffer1Index = 0
		}

		sorted[sortedIndex] = buffer1[buffer1Index]
		buffer1Index++
		sortedIndex++

		// write sorted block to file
		if sortedIndex == 8 {
			err = binary.Write(tempFile, binary.BigEndian, sorted)
			checkError(err)
			sortedIndex = 0
		}
	}

	// account for remaining block 2 elements
	for {
		// load new data if necessary
		if buffer2Index == bufferSize {
			err := binary.Read(r2, binary.BigEndian, buffer2)
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
			err = binary.Write(tempFile, binary.BigEndian, sorted)
			checkError(err)
			sortedIndex = 0
		}
	}


	// move temporary file back to the beginning
	_, err = tempFile.Seek(0,0)
	checkError(err)
	for {
		err := binary.Read(tempFile, binary.BigEndian, sorted)
		if err != nil {
			break
		}
		err = binary.Write(f, binary.BigEndian, sorted)
	}

	_ = tempFile.Close()
}

// writes blockCount blocks of maximum memory worth of random 32-bit integers with values between 0 and maxValue
func RandomIntFileBlocks(blockCount int, filename string, maxValue int) {
	// seed random number generator with time
	rand.Seed(time.Now().UTC().UnixNano())

	// writing memory for testing
	f1, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0600)
	checkError(err)
	currentData := make([]int32, maxMemory/4)

	// write blockCount worth of blocks of maximum memory
	for i := 0; i < blockCount; i++ {
		// generate a random block of data
		for j := 0; j < maxMemory/4; j++ {
			x := int32(rand.Intn(maxValue))
			currentData[j] = x
		}
		// write data to file
		err := binary.Write(f1, binary.BigEndian, currentData)
		checkError(err)
	}
	err = f1.Close()
	checkError(err)
}

// writes intCount random ints with values between 0 and maxValue
func RandomIntFile(intCount int, filename string, maxValue int) {
	// seed random number generator with time
	rand.Seed(time.Now().UTC().UnixNano())

	// writing memory for testing
	f1, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0600)
	checkError(err)

	// generate a random block of data
	for j := 0; j < intCount; j++ {
		x := int32(rand.Intn(maxValue))

		// write data to file
		err := binary.Write(f1, binary.BigEndian, x)
		checkError(err)
	}
	err = f1.Close()
	checkError(err)
}

// prints binary file of 32-bit integers in blocks of size maximum memory
func PrintBinaryIntFile(filename string) {
	f1, err := os.Open(filename)
	checkError(err)
	fInfo, err := f1.Stat()
	checkError(err)
	fileSize := fInfo.Size()
	bytesRead := int64(0)

	bufferSize := int64(maxMemory/4)
	buffer := make([]int32, bufferSize)
	err = binary.Read(f1, binary.BigEndian, buffer)
	bytesRead += (bufferSize * 4)
	fmt.Print(buffer)
	for  bytesRead != fileSize {
		fmt.Print(fileSize)
		fmt.Print(" ")
		fmt.Print(bytesRead)
		fmt.Print(" \n")
		if fileSize - bytesRead < (bufferSize*4) {
			newLength := (fileSize - bytesRead)/4
			buffer =  buffer[0:newLength]
		}
		err = binary.Read(f1, binary.BigEndian, buffer)
		bytesRead += int64(len(buffer)) * 4
		fmt.Print(buffer)
		fmt.Print("\n")
	}
	err = f1.Close()
	checkError(err)
}


func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	RandomIntFile(13,"ints.txt",10000)
	//Sorter("ints.txt")
	PrintBinaryIntFile("ints.txt")
}
