// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

package test

import (
	"io"
	"testing"

	"github.com/NVIDIA/go-tfdata/test/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
	"github.com/NVIDIA/go-tfdata/tfdata/transform/selection"
)

func TestTransform(t *testing.T) {
	const (
		size      = 5000
		number    = "number"
		dupNumber = "dupNumber"
	)
	var (
		sample core.Sample
		err    error
	)

	samplesReader := &testSamplesReader{size: size}

	transformReader := transform.NewSampleTransformer(samplesReader,
		transform.RenameTransformation(number, []string{cntEntry}),
		transform.SampleF(func(sample core.Sample) core.Sample {
			sample[dupNumber] = sample[number]
			return sample
		}),
	)

	cnt := 0
	for sample, err = transformReader.Read(); err == nil; sample, err = transformReader.Read() {
		tassert.Errorf(t, sample[number] != nil, "sample expected to have %s entry", number)
		tassert.Errorf(t, sample[dupNumber] != nil, "sample expected to have %s entry", dupNumber)
		cnt++
	}

	tassert.Errorf(t, err == io.EOF, "expected io.EOF, got %v", err)
	tassert.Errorf(t, cnt == size, "expected to read %d samples, got %d", size, cnt)
}

func TestSelection(t *testing.T) {
	const (
		size      = 5000
		number    = "number"
		dupNumber = "dupNumber"
	)
	var (
		sample core.Sample
		err    error
	)

	samplesReader := &testSamplesReader{size: size}

	transformReader := transform.NewSampleTransformer(samplesReader,
		transform.RenameTransformation(number, []string{cntEntry}),
		transform.SampleF(func(sample core.Sample) core.Sample {
			sample[dupNumber] = sample[number]
			return sample
		}),
		transform.SampleSelections(selection.ByKey(dupNumber)),
	)

	cnt := 0
	for sample, err = transformReader.Read(); err == nil; sample, err = transformReader.Read() {
		tassert.Errorf(t, sample[number] == nil, "sample should not have %s entry", number)
		tassert.Errorf(t, sample[dupNumber] != nil, "sample expected to have %s entry", dupNumber)
		cnt++
	}

	tassert.Errorf(t, err == io.EOF, "expected io.EOF, got %v", err)
	tassert.Errorf(t, cnt == size, "expected to read %d samples, got %d", size, cnt)
}
