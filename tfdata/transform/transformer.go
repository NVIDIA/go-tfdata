// Package tfdata provides interfaces to interact with TFRecord files and TFExamples.
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package transform

import "github.com/NVIDIA/go-tfdata/tfdata/core"

type (
	// Simple - meaning that it does everything locally, doesn't even preload anything
	// However, it's easy to imagine more advanced Reader which has the same Read() interface
	// but underneath prefetches TFExamples, distributes them amongst external workers
	// and after transformations gathers TFExamples and make them available to Read()
	TFExampleTransformer struct {
		reader          core.TFExampleReader
		transformations []TFExampleTransformation
	}

	SamplesTransformer struct {
		reader          core.SampleReader
		transformations []SampleTransformation
	}
)

var (
	_ core.TFExampleReader = &TFExampleTransformer{}
	_ core.SampleReader    = &SamplesTransformer{}
)

func NewTFExampleTransformer(reader core.TFExampleReader, ts ...TFExampleTransformation) core.TFExampleReader {
	return &TFExampleTransformer{
		reader:          reader,
		transformations: ts,
	}
}

func (t *TFExampleTransformer) Read() (*core.TFExample, bool) {
	ex, ok := t.reader.Read()
	if !ok {
		return nil, false
	}
	for _, t := range t.transformations {
		ex = t.TransformTFExample(ex)
	}
	return ex, true
}

func NewSampleTransformer(reader core.SampleReader, ts ...SampleTransformation) core.SampleReader {
	return &SamplesTransformer{
		reader:          reader,
		transformations: ts,
	}
}

func (t *SamplesTransformer) Read() (*core.Sample, bool) {
	sample, ok := t.reader.Read()
	if !ok {
		return nil, false
	}
	for _, t := range t.transformations {
		sample = t.TransformSample(sample)
	}
	return sample, true
}
