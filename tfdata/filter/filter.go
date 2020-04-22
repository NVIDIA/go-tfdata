// Package filter provides implementation of Readers with filter functionality
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package filter

import (
	"github.com/NVIDIA/go-tfdata/tfdata/core"
)

type (
	// Filters empty Samples out of SampleReader
	EmptySamplesReader struct {
		Reader core.SampleReader
	}

	// Filters empty TFExamples out of TFExampleReader
	EmptyTFExamplesReader struct {
		Reader core.TFExampleReader
	}
)

var (
	_ core.TFExampleReader = &EmptyTFExamplesReader{}
	_ core.SampleReader    = &EmptySamplesReader{}
)

// Filter empty samples from reader
func EmptySamples(reader core.SampleReader) core.SampleReader {
	return &EmptySamplesReader{Reader: reader}
}

func (f *EmptySamplesReader) Read() (*core.Sample, bool) {
	sample, ok := f.Reader.Read()
	if !ok {
		return nil, false
	}
	if len(sample.Entries) > 0 {
		return sample, true
	}
	return f.Read()
}

// Filter empty examples from reader
func EmptyExamples(reader core.TFExampleReader) core.TFExampleReader {
	return &EmptyTFExamplesReader{Reader: reader}
}

func (f *EmptyTFExamplesReader) Read() (*core.TFExample, bool) {
	ex, ok := f.Reader.Read()
	if !ok {
		return nil, false
	}
	if len(ex.GetFeatures().Feature) > 0 {
		return ex, true
	}
	return f.Read()
}
