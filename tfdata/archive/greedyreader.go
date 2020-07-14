// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/internal/cmn"
)

func newTarGreedyReader(reader io.Reader) *TarGreedyReader {
	tarReader := &TarGreedyReader{
		rm: NewRecordsManager(),
		r:  tar.NewReader(reader),
		ch: make(chan *sampleResult, 100),
	}

	go func() {
		defer close(tarReader.ch)
		if err := tarReader.prepareRecords(); err != nil {
			// prepareRecords() error will be reported on the first Read()
			tarReader.ch <- &sampleResult{s: nil, err: err}
			return
		}

		for _, r := range tarReader.rm.GetRecords() {
			sample := core.NewSample()
			for k, v := range r.Members {
				sample[k] = v
			}
			sample[core.KeyEntry] = r.Name
			tarReader.ch <- &sampleResult{sample, nil}
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
			buf := bytes.NewBuffer(make([]byte, 0, header.Size))
			n, err := io.Copy(buf, t.r)
			if err != nil && err != io.EOF {
				return err
			}
			if n != header.Size {
				return fmt.Errorf("expected to read %d bytes, read %d instead", header.Size, n)
			}

			t.rm.UpdateRecord(name, ext, buf.Bytes()[:n])
		}
	}
}

func (t *TarGreedyReader) Read() (core.Sample, error) {
	sample, ok := <-t.ch

	if !ok {
		cmn.AssertMsg(sample == nil, "expected nil sample on empty and closed chanel")
		return nil, io.EOF
	}

	cmn.AssertMsg(sample != nil, "expected non-nil sample when chanel was not empty")
	return sample.s, sample.err // nolint staticcheck // nil-check in the assertion above
}
