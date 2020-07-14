// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

package test

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/NVIDIA/go-tfdata/test/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata/archive"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
)

func TestTarReader(t *testing.T) {
	var (
		sample core.Sample
		err    error
	)

	f, err := os.Open("data/small-10.tar")
	tassert.CheckFatal(t, err)

	tr, err := archive.NewTarReader(f)
	tassert.CheckFatal(t, err)

	i := 0
	for sample, err = tr.Read(); err == nil; sample, err = tr.Read() {
		tassert.Errorf(t, len(sample) == 3, "sample expected to have 3 entries") // cls, jpg, __key__
		tassert.Errorf(t, sample["cls"] != nil, "expected cls to be present")
		tassert.Errorf(t, sample["jpg"] != nil, "expected jpg to be present")
		i++
	}

	tassert.Errorf(t, err == io.EOF, "expected io.EOF, got %v", err)
	tassert.Errorf(t, i == 10, "expected tar to have 10 samples, got %d instead", i)
}

func TestTarMnistReader(t *testing.T) {
	var (
		sample core.Sample
		err    error
	)

	f, err := os.Open("data/small-mnist-21.tar")
	tassert.CheckFatal(t, err)

	tr, err := archive.NewTarReader(f)
	tassert.CheckFatal(t, err)

	i := 0
	for sample, err = tr.Read(); err == nil; sample, err = tr.Read() {
		tassert.Errorf(t, len(sample) == 3, "sample expected to have 3 entries") // cls, img, __key__
		tassert.Errorf(t, sample["cls"] != nil, "expected cls to be present")
		clsBytes := sample["cls"].([]byte)
		tassert.Errorf(t, len(clsBytes) == 1, "MNIST class should be a single byte")
		tassert.Errorf(t, clsBytes[0] >= '0' && clsBytes[0] <= '9', "MNIST class should be between 0 and 9")
		tassert.Errorf(t, sample["img"] != nil, "expected jpg to be present")

		i++
	}

	tassert.Errorf(t, err == io.EOF, "expected io.EOF, got %v", err)
	tassert.Errorf(t, i == 21, "expected tar to have 21 samples, got %d instead", i)
}

func TestTarGzReader(t *testing.T) {
	var (
		sample core.Sample
		err    error
	)

	f, err := os.Open("data/small-10.tar.gz")
	tassert.CheckFatal(t, err)

	tr, err := archive.NewTarGzReader(f)
	tassert.CheckFatal(t, err)

	i := 0
	for sample, err = tr.Read(); err == nil; sample, err = tr.Read() {
		tassert.Errorf(t, len(sample) == 3, "sample expected to have 3 entries") // cls, jpg, __key__
		tassert.Errorf(t, sample["cls"] != nil, "expected cls to be present")
		tassert.Errorf(t, sample["jpg"] != nil, "expected jpg to be present")
		i++
	}

	tassert.Errorf(t, err == io.EOF, "expected io.EOF, got %v", err)
	tassert.Errorf(t, i == 10, "expected tar to have 10 samples, got %d instead", i)
}

func TestInvalidGreedyTarError(t *testing.T) {
	b := bytes.NewBuffer(nil)
	b.Write([]byte("invalid TAR"))

	r, err := archive.NewTarReader(b)
	tassert.Fatalf(t, r != nil, "unexpected nil from TarReader")
	// The error will come on the first Read() request, as TAR is processed asynchronously.
	tassert.Fatalf(t, err == nil, "expected calling TarReader to be successful")
	_, ok := r.(*archive.TarGreedyReader)
	// Make sure that we test
	tassert.Fatalf(t, ok, "expected bytes.Buffer to not implement io.Seeker and be used as TarGreedyReader")

	_, err = r.Read()
	tassert.Fatalf(t, err != nil && err != io.EOF, "expected TAR read failure, got %v", err)
}

func TestInvalidSeekTarError(t *testing.T) {
	f, err := ioutil.TempFile("", "seek.tar")
	tassert.CheckFatal(t, err)
	f.Write([]byte("invalid TAR"))

	r, err := archive.NewTarReader(f)
	tassert.CheckFatal(t, err)
	_, ok := r.(*archive.TarSeekReader)
	tassert.Fatalf(t, ok, "expected os.File to be used as TarSeekReader")

	_, err = r.Read()
	tassert.Fatalf(t, err != nil && err != io.EOF, "expected TAR read failure, got %v", err)
}
