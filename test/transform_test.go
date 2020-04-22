//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package test

import (
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

	samplesReader := &testSamplesReader{size: size}

	transformReader := transform.NewSampleTransformer(samplesReader,
		transform.RenameTransformation(number, []string{cntEntry}),
		transform.SampleF(func(sample *core.Sample) *core.Sample {
			sample.Entries[dupNumber] = sample.Entries[number]
			return sample
		}),
	)

	cnt := 0
	for sample, ok := transformReader.Read(); ok; sample, ok = transformReader.Read() {
		tassert.Errorf(t, sample.Entries[number] != nil, "sample expected to have %s entry", number)
		tassert.Errorf(t, sample.Entries[dupNumber] != nil, "sample expected to have %s entry", dupNumber)
		cnt++
	}

	tassert.Errorf(t, cnt == size, "expected to read %d samples, got %d", size, cnt)
}

func TestSelection(t *testing.T) {
	const (
		size      = 5000
		number    = "number"
		dupNumber = "dupNumber"
	)

	samplesReader := &testSamplesReader{size: size}

	transformReader := transform.NewSampleTransformer(samplesReader,
		transform.RenameTransformation(number, []string{cntEntry}),
		transform.SampleF(func(sample *core.Sample) *core.Sample {
			sample.Entries[dupNumber] = sample.Entries[number]
			return sample
		}),
		transform.SampleSelections(selection.ByKey(dupNumber)),
	)

	cnt := 0
	for sample, ok := transformReader.Read(); ok; sample, ok = transformReader.Read() {
		tassert.Errorf(t, sample.Entries[number] == nil, "sample should not have %s entry", number)
		tassert.Errorf(t, sample.Entries[dupNumber] != nil, "sample expected to have %s entry", dupNumber)
		cnt++
	}

	tassert.Errorf(t, cnt == size, "expected to read %d samples, got %d", size, cnt)
}
