// Package archive contains tools for transition between TAR files and SampleReader
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package archive

import (
	"archive/tar"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
)

// TODO: rethink the whole extraction to memory logic... make use of AIS dsort/extract (?)
// At the moment we have two types of readers: Greedy and Seek
// Greedy reads the whole Tar/TarGZ and then makes Samples available to read
// This is versatile approach when we can't iterate twice over the reader (without coping it etx)
// Seek reader is optimization for a use case when io.Reader is actually io.SeekReader as well,
// meaning that we can read it twice. In this case, we can first gather Tar/TarGz metadata
// (like all name files) and during the second run we are able to detect immediately when a new Sample is ready
// to be Read, because there are no more files, relevant to the Sample. in the remaining Tar bytes.

type (
	TarSeekReader struct {
		mtx                sync.Mutex
		recordsManager     RecordsManager
		recordsMetaManager RecordsManager
		r                  *tar.Reader
	}

	TarGreedyReader struct {
		rm RecordsManager
		r  *tar.Reader
		ch chan *core.Sample
	}
)

var (
	_ core.SampleReader = &TarGreedyReader{}
	_ core.SampleReader = &TarSeekReader{}
)

func NewTarReader(reader io.Reader) (core.SampleReader, error) {
	if readSeeker, ok := reader.(io.ReadSeeker); ok {
		return newTarSeekReader(readSeeker)
	}
	return newTarGreedyReader(reader), nil
}

func NewTarGzReader(reader io.Reader) (core.SampleReader, error) {
	if readSeeker, ok := reader.(io.ReadSeeker); ok {
		return newTarGzSeekReader(readSeeker)
	}
	return newTarGzGreedyReader(reader)
}

func nameExtFromHeader(header *tar.Header) (name, ext string) {
	ext = filepath.Ext(header.Name)
	name = strings.TrimSuffix(header.Name, ext)
	ext = strings.TrimPrefix(ext, ".")
	return name, ext
}
