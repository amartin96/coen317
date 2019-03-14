package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/amartin96/coen317/common"
	"math/rand"
	"os"
	"time"
)

func CreateFile(name string, size int, max int) {
	// create file
	file, err := os.Create(name)
	common.PanicOnError(err)
	defer common.Close(file)

	// create RNG
	rng := rand.New(rand.NewSource(time.Now().Unix()))

	// generate random numbers and write them to the file
	for i := 0; i < size; i++ {
		common.PanicOnError(binary.Write(file, binary.BigEndian, int32(rng.Intn(max))))
	}
}

func main() {
	var args struct {
		Name string
		Size int
		Max  int
	}
	flag.StringVar(&args.Name, "name", "", "Name of file to be created")
	flag.IntVar(&args.Size, "size", 0, "Number of integers to be generated")
	flag.IntVar(&args.Max, "max", 0, "Max integer value")
	flag.Parse()
	if args.Name == "" || args.Size == 0 || args.Max == 0 {
		fmt.Printf("Usage: %v -name <filename> -size <# ints> -max <max int value>\n", os.Args[0])
		return
	}

	CreateFile(args.Name, args.Size, args.Max)
}
