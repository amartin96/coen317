package Merge

import (
	"coen317/common"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

// sort a file of integers on machines with maximum memory equal to maxMemory
func Sorter(filename string) {
	f1, err := os.OpenFile(filename, os.O_RDWR, 0600)
	common.PanicOnError(err)
	f2, err := os.OpenFile(filename, os.O_RDWR, 0600)
	common.PanicOnError(err)
	f3, err := os.OpenFile(filename, os.O_RDWR, 0600)
	common.PanicOnError(err)
	fInfo, err := f1.Stat()
	common.PanicOnError(err)
	fileSize := fInfo.Size()
	var blockSizeBinary int64 = common.BUFSIZE
	var blockSizeInt int64 = common.BUFSIZE / 4

	// perform quicksort on blocks of the maximum memory size
	buffer := make([]int32, blockSizeInt)
	var bytesRead int64 = 0
	for {
		// check if completed
		if bytesRead == fileSize {
			break
		}

		// get current position of file pointer
		f1Pos, err := f1.Seek(0, io.SeekCurrent)
		common.PanicOnError(err)

		// shrink buffer if need be
		if (fileSize - bytesRead) < blockSizeBinary {
			blockSizeBinary = fileSize - bytesRead
			blockSizeInt = blockSizeBinary / 4
			buffer = buffer[0:blockSizeInt]
		}

		// read in a block at a time
		err = binary.Read(f1, binary.BigEndian, buffer)
		common.PanicOnError(err)
		bytesRead += blockSizeBinary

		// sort the block
		sort.SliceStable(buffer, func(t, p int) bool { return buffer[t] < buffer[p] })

		// seek back so we can re-write the block
		_, err = f1.Seek(f1Pos, io.SeekStart)
		common.PanicOnError(err)

		// write the block back
		err = binary.Write(f1, binary.BigEndian, buffer)
		common.PanicOnError(err)
	}

	// do the merging
	blockSizeBinary = common.BUFSIZE
	var blockCount int64
	var curBlock int64
	var block1Size int64
	var block2Size int64
	for blockSizeBinary < fileSize {
		blockCount = fileSize / blockSizeBinary
		block1Size = blockSizeBinary
		block2Size = blockSizeBinary
		curBlock = 0
		_, err = f1.Seek(0, io.SeekStart)
		common.PanicOnError(err)
		_, err = f2.Seek(blockSizeBinary, io.SeekStart)
		common.PanicOnError(err)
		_, err = f3.Seek(0, io.SeekStart)
		common.PanicOnError(err)
		for curBlock < blockCount {
			// get limit readers of current blockSize
			limitR1 := io.LimitReader(f1, blockSizeBinary)
			limitR2 := io.LimitReader(f2, blockSizeBinary)

			// block1 will be shorter than normal
			if fileSize-(curBlock*blockSizeBinary) < blockSizeBinary {
				block1Size = fileSize - (curBlock * blockSizeBinary)
				block2Size = 0
			} else if fileSize-((curBlock+1)*blockSizeBinary) < blockSizeBinary {
				block2Size = fileSize - ((curBlock + 1) * blockSizeBinary)
			}

			// pass limit readers to have content merged together
			Merge(limitR1, limitR2, block1Size, block2Size, f3)

			// seek file pointers to be point at next blocks to be merged
			_, err := f1.Seek(blockSizeBinary, 1)
			common.PanicOnError(err)
			_, err = f2.Seek(blockSizeBinary, 1)
			common.PanicOnError(err)

			// progress current block to next two blocks to be merged
			curBlock += 2
		}
		// double block size for next merging step
		blockSizeBinary *= 2
	}
}

// perform actual merging of blocks
func Merge(r1 io.Reader, r2 io.Reader, r1Size int64, r2Size int64, f io.Writer) {
	// buffers for reading in the binary
	buffer1SizeInt := common.BUFSIZE / 8
	buffer1SizeBinary := common.BUFSIZE / 2
	buffer2SizeInt := common.BUFSIZE / 8
	buffer2SizeBinary := common.BUFSIZE / 2
	sortedSizeInt := common.BUFSIZE / 4
	sortedSizeBinary := common.BUFSIZE
	buffer1 := make([]int32, buffer1SizeInt)
	buffer2 := make([]int32, buffer2SizeInt)
	sorted := make([]int32, sortedSizeInt)
	tempFile, err := ioutil.TempFile("", "temp")
	common.PanicOnError(err)

	// indices and counters for arrays
	buffer1Index := 0
	buffer2Index := 0
	sortedIndex := 0
	var r1BytesRead int64 = 0
	var r2BytesRead int64 = 0

	if r1BytesRead != r1Size && int(r1Size-r1BytesRead) < buffer1SizeBinary {
		buffer1SizeBinary = int(r1Size - r1BytesRead)
		buffer1SizeInt = buffer1SizeBinary / 4
		buffer1 = buffer1[0:buffer1SizeInt]
	}
	err = binary.Read(r1, binary.BigEndian, buffer1)
	common.PanicOnError(err)
	r1BytesRead += int64(buffer1SizeBinary)

	if r2BytesRead != r2Size && int(r2Size-r2BytesRead) < buffer2SizeBinary {
		buffer2SizeBinary = int(r2Size - r2BytesRead)
		buffer2SizeInt = buffer2SizeBinary / 4
		buffer2 = buffer2[0:buffer2SizeInt]
	}
	err = binary.Read(r2, binary.BigEndian, buffer2)
	common.PanicOnError(err)
	r2BytesRead += int64(buffer2SizeBinary)

	// main merging algorithm
	for {
		// load new data if necessary
		if buffer1Index == buffer1SizeInt {
			if r1BytesRead == r1Size {
				break
			}
			if int(r1Size-r1BytesRead) < buffer1SizeBinary {
				buffer1SizeBinary = int(r1Size - r1BytesRead)
				buffer1SizeInt = buffer1SizeBinary / 4
				buffer1 = buffer1[0:buffer1SizeInt]
			}
			err := binary.Read(r1, binary.BigEndian, buffer1)
			if err != nil {
				break
			}
			r1BytesRead += int64(buffer1SizeBinary)
			buffer1Index = 0
		} else if buffer2Index == buffer2SizeInt {
			if r2BytesRead == r2Size {
				break
			}
			if int(r2Size-r2BytesRead) < buffer2SizeBinary {
				buffer2SizeBinary = int(r2Size - r2BytesRead)
				buffer2SizeInt = buffer2SizeBinary / 4
				buffer2 = buffer2[0:buffer2SizeInt]
			}
			err := binary.Read(r2, binary.BigEndian, buffer2)
			common.PanicOnError(err)
			r2BytesRead += int64(buffer2SizeBinary)
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
		if sortedIndex == sortedSizeInt {
			err = binary.Write(tempFile, binary.BigEndian, sorted)
			common.PanicOnError(err)
			sortedIndex = 0
		}
	}

	// account for remaining block 1 elements
	for {
		// load new data if necessary
		if buffer1Index == buffer1SizeInt {
			if r1BytesRead == r1Size {
				break
			}
			if int(r1Size-r1BytesRead) < buffer1SizeBinary {
				buffer1SizeBinary = int(r1Size - r1BytesRead)
				buffer1SizeInt = buffer1SizeBinary / 4
				buffer1 = buffer1[0:buffer1SizeInt]
			}
			err := binary.Read(r1, binary.BigEndian, buffer1)
			common.PanicOnError(err)
			r1BytesRead += int64(buffer1SizeBinary)
			buffer1Index = 0
		}

		sorted[sortedIndex] = buffer1[buffer1Index]
		buffer1Index++
		sortedIndex++

		// write sorted block to file
		if sortedIndex == sortedSizeInt {
			err = binary.Write(tempFile, binary.BigEndian, sorted)
			common.PanicOnError(err)
			sortedIndex = 0
		}
	}

	// account for remaining block 2 elements
	for {
		// load new data if necessary
		if buffer2Index == buffer2SizeInt {
			if r2BytesRead == r2Size {
				break
			}
			if int(r2Size-r2BytesRead) < buffer2SizeBinary {
				buffer2SizeBinary = int(r2Size - r2BytesRead)
				buffer2SizeInt = buffer2SizeBinary / 4
				buffer2 = buffer2[0:buffer2SizeInt]
			}
			err := binary.Read(r2, binary.BigEndian, buffer2)
			common.PanicOnError(err)
			r2BytesRead += int64(buffer2SizeBinary)
			buffer2Index = 0
		}
		sorted[sortedIndex] = buffer2[buffer2Index]
		buffer2Index++
		sortedIndex++

		// write sorted block to file
		if sortedIndex == sortedSizeInt {
			err = binary.Write(tempFile, binary.BigEndian, sorted)
			common.PanicOnError(err)
			sortedIndex = 0
		}
	}

	// write any remaining leftover bytes
	if sortedIndex != 0 {
		err = binary.Write(tempFile, binary.BigEndian, sorted[0:sortedIndex])
		common.PanicOnError(err)
	}

	// move temporary file back to the beginning
	_, err = tempFile.Seek(0, io.SeekStart)
	common.PanicOnError(err)
	tempFileInfo, _ := tempFile.Stat()
	tempFileSize := tempFileInfo.Size()
	tempBytesRead := int64(0)
	for {
		if tempBytesRead == tempFileSize {
			break
		}
		if int(tempFileSize-tempBytesRead) < sortedSizeBinary {
			sortedSizeBinary = int(tempFileSize - tempBytesRead)
			sortedSizeInt = sortedSizeBinary / 4
			sorted = sorted[0:sortedSizeInt]
		}
		err := binary.Read(tempFile, binary.BigEndian, sorted)
		common.PanicOnError(err)
		tempBytesRead += int64(sortedSizeBinary)
		err = binary.Write(f, binary.BigEndian, sorted)
		common.PanicOnError(err)
	}

	_ = tempFile.Close()
}

// prints binary file of 32-bit integers in blocks of size maximum memory
func PrintBinaryIntFile(filename string) {
	f1, err := os.Open(filename)
	common.PanicOnError(err)
	fInfo, err := f1.Stat()
	common.PanicOnError(err)
	fileSize := fInfo.Size()
	bytesRead := int64(0)

	bufferSize := int64(common.BUFSIZE)
	buffer := make([]int32, bufferSize/4)
	if (fileSize - bytesRead) < bufferSize {
		bufferSize = fileSize - bytesRead
		buffer = buffer[0 : bufferSize/4]
	}
	err = binary.Read(f1, binary.BigEndian, buffer)
	bytesRead += bufferSize
	fmt.Println(buffer)
	for bytesRead != fileSize {
		if (fileSize - bytesRead) < bufferSize {
			bufferSize = fileSize - bytesRead
			buffer = buffer[0 : bufferSize/4]
		}
		err = binary.Read(f1, binary.BigEndian, buffer)
		fmt.Println(buffer)
		bytesRead += bufferSize
	}
	err = f1.Close()
	common.PanicOnError(err)
}
