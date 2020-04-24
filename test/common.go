//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package test

import (
	"encoding/binary"
	"sync"
	"time"

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

func (t *testTFExamplesReader) Read() (*core.TFExample, bool) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	if t.readCnt == t.size {
		return nil, false
	}
	ex := core.NewTFExample()
	ex.AddInt(cntEntry, t.readCnt)
	t.readCnt++
	return ex, true
}

func (t *testSamplesReader) Read() (*core.Sample, bool) {
	if t.readCnt == t.size {
		return nil, false
	}

	buf := make([]byte, 8)
	binary.PutVarint(buf, int64(t.readCnt))
	sample := core.NewSample(time.Now().String())
	sample.Entries[cntEntry] = buf
	t.readCnt++
	return sample, true
}
