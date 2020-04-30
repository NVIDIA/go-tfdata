//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package test

import (
	"io"
	"os"
	"testing"

	"github.com/NVIDIA/go-tfdata/test/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata/archive"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
)

func TestTarReader(t *testing.T) {
	var (
		sample *core.Sample
		err    error
	)

	f, err := os.Open("data/small-10.tar")
	tassert.CheckFatal(t, err)

	tr, err := archive.NewTarReader(f)
	tassert.CheckFatal(t, err)

	i := 0
	for sample, err = tr.Read(); err == nil; sample, err = tr.Read() {
		tassert.Errorf(t, len(sample.Entries) == 3, "sample expected to have 3 entries") // cls, jpg, __key__
		tassert.Errorf(t, sample.Entries["cls"] != nil, "expected cls to be present")
		tassert.Errorf(t, sample.Entries["jpg"] != nil, "expected jpg to be present")
		i++
	}

	tassert.Errorf(t, err == io.EOF, "expected io.EOF, got %v", err)
	tassert.Errorf(t, i == 10, "expected tar to have 10 samples, got %d instead", i)
}

func TestTarGzReader(t *testing.T) {
	var (
		sample *core.Sample
		err    error
	)

	f, err := os.Open("data/small-10.tar.gz")
	tassert.CheckFatal(t, err)

	tr, err := archive.NewTarGzReader(f)
	tassert.CheckFatal(t, err)

	i := 0
	for sample, err = tr.Read(); err == nil; sample, err = tr.Read() {
		tassert.Errorf(t, len(sample.Entries) == 3, "sample expected to have 3 entries") // cls, jpg, __key__
		tassert.Errorf(t, sample.Entries["cls"] != nil, "expected cls to be present")
		tassert.Errorf(t, sample.Entries["jpg"] != nil, "expected jpg to be present")
		i++
	}

	tassert.Errorf(t, err == io.EOF, "expected io.EOF, got %v", err)
	tassert.Errorf(t, i == 10, "expected tar to have 10 samples, got %d instead", i)
}
