//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package test

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/NVIDIA/go-tfdata/test/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/filter"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
	"github.com/NVIDIA/go-tfdata/tfdata/transform/selection"
)

func TestFilter(t *testing.T) {
	const size = 5000
	var (
		sample *core.Sample
		err    error
	)

	samplesReader := &testSamplesReader{size: size}
	buf2500, buf3500 := make([]byte, 8), make([]byte, 8)
	binary.PutVarint(buf2500, int64(2500))
	binary.PutVarint(buf3500, int64(3500))

	transformReader := transform.NewSampleTransformer(samplesReader,
		// Select only entries which have value 2500 or 3500 for key cntEntry
		transform.SampleSelections(
			selection.ByKeyValue(cntEntry, buf2500),
			selection.ByKeyValue(cntEntry, buf3500),
		))
	// filter empty examples - those which didn't have 2500 or 3500 value in cntEntry
	filterTransformReader := filter.EmptySamples(transformReader)

	cnt := 0
	for sample, err = filterTransformReader.Read(); err == nil; sample, err = filterTransformReader.Read() {
		tassert.Fatalf(t, sample.Entries[cntEntry] != nil, "sample should have %s entry", cntEntry)
		tassert.Fatalf(t, len(sample.Entries) == 1, "sample expected to have only %s entry: %v", cntEntry, sample.Entries)
		cnt++
	}

	tassert.Errorf(t, err == io.EOF, "expected io.EOF, got %v", err)
	tassert.Errorf(t, cnt == 2, "expected to read 2 samples, got %d", cnt)
}
