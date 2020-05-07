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
	"github.com/NVIDIA/go-tfdata/tfdata/internal/cmn"
)

func newTarSeekReader(reader io.ReadSeeker) (*TarSeekReader, error) {
	tarReader := &TarSeekReader{
		recordsManager:     NewRecordsManager(),
		recordsMetaManager: NewRecordsManager(),
		r:                  tar.NewReader(reader),
	}

	err := tarReader.prepareMeta()
	if err != nil {
		return nil, err
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	tarReader.r = tar.NewReader(reader)

	return tarReader, nil
}

func newTarGzSeekReader(reader io.ReadSeeker) (*TarSeekReader, error) {
	gzr, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}

	tarReader := &TarSeekReader{
		recordsManager:     NewRecordsManager(),
		recordsMetaManager: NewRecordsManager(),
		r:                  tar.NewReader(gzr),
	}

	err = tarReader.prepareMeta()
	if err != nil {
		return nil, err
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err == nil {
		gzr, err = gzip.NewReader(reader)
	}
	if err != nil {
		return nil, err
	}
	tarReader.r = tar.NewReader(gzr)

	return tarReader, err
}

func (t *TarSeekReader) prepareMeta() error {
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
			t.recordsMetaManager.UpdateRecord(name, ext, nil)
		}
	}
}

func (t *TarSeekReader) Read() (sample *core.Sample, err error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	// iterate until first tar record is ready or EOF
	for {
		header, err := t.r.Next()

		switch {
		case err == io.EOF:
			cmn.Assert(t.recordsManager.Len() == 0)
			cmn.Assert(t.recordsMetaManager.Len() == 0)
			return nil, io.EOF
		case err != nil:
			return nil, err
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
				return nil, err
			}
			if n != header.Size {
				return nil, fmt.Errorf("expected to read %d bytes, read %d instead", header.Size, n)
			}

			t.recordsManager.UpdateRecord(name, ext, buf.Bytes()[:n])
			if t.recordsMetaManager.GetRecord(name).SameMembers(t.recordsManager.GetRecord(name)) {
				record := t.recordsManager.GetRecord(name)
				sample = core.NewSample()
				for k, v := range record.Members {
					sample.Entries[k] = v
				}
				sample.Entries[core.KeyEntry] = record.Name
				t.recordsMetaManager.DeleteRecord(name)
				t.recordsManager.DeleteRecord(name)
				return sample, nil
			}
		}
	}
}
