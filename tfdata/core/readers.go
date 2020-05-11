// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

package core

import "io"

type (
	// TFExampleReader returns io.EOF if there's nothing left to be read
	TFExampleReader interface {
		Read() (ex *TFExample, err error)
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

	// SampleReader returns io.EOF if there's nothing left to be read
	SampleReader interface {
		Read() (sample Sample, err error)
	}

	SampleWriter interface {
		Write(s Sample) error
		Close()
	}

	SampleReadWriter interface {
		SampleReader
		SampleWriter
	}

	// SampleChannel is simple implementation of SampleReader
	SampleChannel struct {
		ch chan Sample
	}
)

var (
	_ TFExampleReadWriter = &TFExampleChannel{}
	_ SampleReadWriter    = &SampleChannel{}
)

// TFExampleReaders / TFExampleWriters

func NewTFExampleChannel(bufSize int) *TFExampleChannel {
	return &TFExampleChannel{ch: make(chan *TFExample, bufSize)}
}

func (c *TFExampleChannel) Read() (*TFExample, error) {
	ex, ok := <-c.ch
	if !ok {
		return ex, io.EOF
	}
	return ex, nil
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
	return &SampleChannel{ch: make(chan Sample, bufSize)}
}

func (c *SampleChannel) Read() (Sample, error) {
	sample, ok := <-c.ch
	if !ok {
		return sample, io.EOF
	}
	return sample, nil
}

func (c *SampleChannel) Write(sample Sample) error {
	c.ch <- sample
	return nil
}

func (c *SampleChannel) Close() {
	close(c.ch)
}
