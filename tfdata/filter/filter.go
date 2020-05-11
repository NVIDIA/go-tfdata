// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

// Package filter provides implementation of Readers with filter functionality.
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

// Filter empty Samples from reader. If a Sample has only __key__ entry, it is treated as an empty.
func EmptySamples(reader core.SampleReader) core.SampleReader {
	return &EmptySamplesReader{Reader: reader}
}

func (f *EmptySamplesReader) Read() (core.Sample, error) {
	sample, err := f.Reader.Read()
	if err != nil {
		return nil, err
	}
	if !isSampleEmpty(sample) {
		return sample, nil
	}
	return f.Read()
}

// Filter empty TFExamples from reader. If a TFExample has only __key__ entry, it is treated as an empty.
func EmptyExamples(reader core.TFExampleReader) core.TFExampleReader {
	return &EmptyTFExamplesReader{Reader: reader}
}

func (f *EmptyTFExamplesReader) Read() (*core.TFExample, error) {
	ex, err := f.Reader.Read()
	if err != nil {
		return nil, err
	}
	if !isTFExampleEmpty(ex) {
		return ex, nil
	}
	return f.Read()
}

func isSampleEmpty(sample core.Sample) bool {
	if len(sample) == 0 {
		return true
	}
	if len(sample) == 1 && sample[core.KeyEntry] != nil {
		return true
	}

	return false
}

func isTFExampleEmpty(ex *core.TFExample) bool {
	if len(ex.GetFeatures().Feature) == 0 {
		return true
	}
	if len(ex.GetFeatures().Feature) == 1 && ex.GetFeatures().Feature[core.KeyEntry] != nil {
		return true
	}

	return false
}
