// Package tfdata provides interfaces to interact with TFRecord files and TFExamples.
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package transform

import "github.com/NVIDIA/go-tfdata/tfdata/core"

type (
	// TFExampleTransformer - it does everything locally, doesn't even preload anything from internal reader.
	// However, it's easy to imagine more advanced Transformer (Reader) which has the same Read() interface,
	// but underneath prefetches TFExamples, distributes them amongst external workers
	// and after transformations gathers TFExamples and make them available to Read()
	TFExampleTransformer struct {
		reader          core.TFExampleReader
		transformations []TFExampleTransformation
	}

	// Transforms SamplesReader based on given transformations
	SamplesTransformer struct {
		reader          core.SampleReader
		transformations []SampleTransformation
	}

	// Default SamplesToTFExamples transformer: put into TFExample each of Sample entries as BytesList
	SamplesToTFExamplesTransformer struct {
		reader core.SampleReader
	}
)

var (
	_, _ core.TFExampleReader = &TFExampleTransformer{}, &SamplesToTFExamplesTransformer{}
	_    core.SampleReader    = &SamplesTransformer{}
)

// NewTFExampleTransformer consumes TFExampleReader, applies transformations in order of occurrence, produces TFExampleReader.
func NewTFExampleTransformer(reader core.TFExampleReader, ts ...TFExampleTransformation) core.TFExampleReader {
	return &TFExampleTransformer{
		reader:          reader,
		transformations: ts,
	}
}

func (t *TFExampleTransformer) Read() (*core.TFExample, error) {
	ex, err := t.reader.Read()
	if err != nil {
		return nil, err
	}
	for _, t := range t.transformations {
		ex = t.TransformTFExample(ex)
	}
	return ex, nil
}

// NewSampleTransformer consumes TFExampleReader, applies transformations in order of occurrence, produces SampleReader.
func NewSampleTransformer(reader core.SampleReader, ts ...SampleTransformation) core.SampleReader {
	return &SamplesTransformer{
		reader:          reader,
		transformations: ts,
	}
}

func (t *SamplesTransformer) Read() (*core.Sample, error) {
	sample, err := t.reader.Read()
	if err != nil {
		return nil, err
	}
	for _, t := range t.transformations {
		sample = t.TransformSample(sample)
	}
	return sample, nil
}

// NewSamplesToTFExample consumes SampleReader, applies default Sample to TFExample conversion, produces TFExampleReader.
// Default Sample to TFExample conversion is put into TFExample each of Sample entries as BytesList
func NewSamplesToTFExample(reader core.SampleReader) *SamplesToTFExamplesTransformer {
	return &SamplesToTFExamplesTransformer{reader: reader}
}

func (t *SamplesToTFExamplesTransformer) Read() (*core.TFExample, error) {
	sample, err := t.reader.Read()
	if err != nil {
		return nil, err
	}

	example := core.NewTFExample()
	for k, v := range sample.Entries {
		example.AddBytes(k, v)
	}
	return example, nil
}
