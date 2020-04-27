//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package test

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
)

const cntEntry = "cnt"

type (
	testTFExamplesReader struct {
		mtx           sync.Mutex
		readCnt, size int
	}

	testSamplesReader struct {
		readCnt, size int
	}
)

func (t *testTFExamplesReader) Read() (*core.TFExample, error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	if t.readCnt == t.size {
		return nil, io.EOF
	}
	ex := core.NewTFExample()
	ex.AddInt(cntEntry, t.readCnt)
	t.readCnt++
	return ex, nil
}

func (t *testSamplesReader) Read() (*core.Sample, error) {
	if t.readCnt == t.size {
		return nil, io.EOF
	}

	buf := make([]byte, 8)
	binary.PutVarint(buf, int64(t.readCnt))
	sample := core.NewSample()
	sample.Entries[cntEntry] = buf
	t.readCnt++
	return sample, nil
}
