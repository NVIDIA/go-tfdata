//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package test

import (
	"os"
	"testing"

	"github.com/NVIDIA/go-tfdata/test/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata/archive"
)

func TestTarReader(t *testing.T) {
	f, err := os.Open("data/small-10.tar")
	tassert.CheckFatal(t, err)

	tr, err := archive.NewTarReader(f)
	tassert.CheckFatal(t, err)

	i := 0
	for sample, ok := tr.Read(); ok; sample, ok = tr.Read() {
		tassert.Errorf(t, len(sample.Entries) == 2, "sample expected to have 2 entries")
		tassert.Errorf(t, sample.Entries["cls"] != nil, "expected cls to be present")
		tassert.Errorf(t, sample.Entries["jpg"] != nil, "expected jpg to be present")
		i++
	}

	tassert.Errorf(t, i == 10, "expected tar to have 10 samples, got %d instead", i)
}
