//// Package archive contains tools for transition between TAR files and SampleReader
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
)

func newTarGreedyReader(reader io.Reader) *TarGreedyReader {
	tarReader := &TarGreedyReader{
		rm: NewRecordsManager(),
		r:  tar.NewReader(reader),
		ch: make(chan *core.Sample, 100),
	}

	go func() {
		defer close(tarReader.ch)
		if err := tarReader.prepareRecords(); err != nil {
			return
		}

		for _, r := range tarReader.rm.GetRecords() {
			tarReader.ch <- core.NewSample(r.Name, r.Members)
		}
	}()

	return tarReader
}

func newTarGzGreedyReader(reader io.Reader) (*TarGreedyReader, error) {
	gzr, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}

	return &TarGreedyReader{
		rm: NewRecordsManager(),
		r:  tar.NewReader(gzr),
	}, nil
}

func (t *TarGreedyReader) prepareRecords() error {
	for {
		header, err := t.r.Next()

		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		case header == nil:
			continue
		}

		name, ext := nameExtFromHeader(header)

		switch header.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
			buf := make([]byte, header.Size)
			n, err := io.Copy(bytes.NewBuffer(buf), t.r)
			if err != nil && err != io.EOF {
				return err
			}
			if n != header.Size {
				return fmt.Errorf("expected to read %d bytes, read %d instead", header.Size, n)
			}

			t.rm.UpdateRecord(name, ext, buf)
		}
	}
}

func (t *TarGreedyReader) Read() (*core.Sample, error) {
	sample, ok := <-t.ch
	if !ok {
		return sample, io.EOF
	}
	return sample, nil
}
