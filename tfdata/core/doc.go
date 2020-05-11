// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

// Package core provides basic types, interfaces and functions to interact with
// Samples extracted from TAR archives, TFExamples and TFRecords.
package core

import (
	"fmt"
	"os"
)

func Example_readTFRecord() {
	input, _ := os.Open("source.tfrecord")
	reader := NewTFRecordReader(input)

	for ex, err := reader.Read(); err == nil; ex, err = reader.Read() {
		fmt.Print(ex)
	}
}

func Example_writeTFRecord() {
	output, _ := os.Create("dest.tfrecord")
	writer := NewTFRecordWriter(output)

	ex := NewTFExample()
	ex.AddInt64("x", 1)
	ex.AddFloat("y", 0.1)
	writer.WriteExample(ex)
}
