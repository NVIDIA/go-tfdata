// Package tfdata provides interfaces to interact with TFRecord files and TFExamples.
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package core

type (
	// TFExampleReader / TFExampleWriter
	TFExampleReader interface {
		Read() (ex *TFExample, ok bool)
	}

	TFExampleWriter interface {
		Write(ex *TFExample) error
		Close()
	}

	TFExampleReadWriter interface {
		TFExampleReader
		TFExampleWriter
	}

	TFExampleChannel struct {
		ch chan *TFExample
	}

	// SampleReader / SampleWriter
	SampleReader interface {
		Read() (sample *Sample, ok bool)
	}

	SampleWriter interface {
		Write(s *Sample) error
		Close()
	}

	SampleReadWriter interface {
		SampleReader
		SampleWriter
	}

	SampleChannel struct {
		ch chan *Sample
	}

	SamplesToTFExamplesTransformer struct {
		reader SampleReader
	}
)

var (
	_ TFExampleReadWriter = &TFExampleChannel{}
	_ TFExampleReader     = &SamplesToTFExamplesTransformer{}

	_ SampleReadWriter = &SampleChannel{}
)

// TFExampleReaders / TFExampleWriters

func NewTFExampleChannel(bufSize int) *TFExampleChannel {
	return &TFExampleChannel{ch: make(chan *TFExample, bufSize)}
}

func (c *TFExampleChannel) Read() (*TFExample, bool) {
	ex, ok := <-c.ch
	return ex, ok
}

func (c *TFExampleChannel) Write(example *TFExample) error {
	c.ch <- example
	return nil
}

func (c *TFExampleChannel) Close() {
	close(c.ch)
}

// SampleReaders / SampleWriters

func NewSampleChannel(bufSize int) *SampleChannel {
	return &SampleChannel{ch: make(chan *Sample, bufSize)}
}

func (c *SampleChannel) Read() (*Sample, bool) {
	ex, ok := <-c.ch
	return ex, ok
}

func (c *SampleChannel) Write(sample *Sample) error {
	c.ch <- sample
	return nil
}

func (c *SampleChannel) Close() {
	close(c.ch)
}

func NewSamplesToTFExampleTransformer(reader SampleReader) *SamplesToTFExamplesTransformer {
	return &SamplesToTFExamplesTransformer{reader: reader}
}

func (t *SamplesToTFExamplesTransformer) Read() (ex *TFExample, ok bool) {
	sample, ok := t.reader.Read()
	if !ok {
		return nil, false
	}

	example := NewTFExample()
	for k, v := range sample.Entries {
		example.AddBytes(k, v)
	}
	return example, true
}
