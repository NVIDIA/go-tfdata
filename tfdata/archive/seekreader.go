//// Package archive contains tools for transition between TAR files and SampleReader
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package archive

import (
	"archive/tar"
	"compress/gzip"
	"io"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/internal/cmn"
	"github.com/golang/glog"
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

	err = tarReader.prepareMeta()
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

func (t *TarSeekReader) Read() (sample *core.Sample, ok bool) {
	// iterate until first tar record is ready or EOF
	for {
		header, err := t.r.Next()

		switch {
		case err == io.EOF:
			cmn.Assert(t.recordsManager.Len() == 0)
			cmn.Assert(t.recordsMetaManager.Len() == 0)
			return nil, false
		case err != nil:
			glog.Error(err)
			return nil, false
		case header == nil:
			continue
		}

		name, ext := nameExtFromHeader(header)

		switch header.Typeflag {
		case tar.TypeDir:
			continue

		case tar.TypeReg:
			buff := make([]byte, header.Size)
			n, err := t.r.Read(buff)
			if err != nil && err != io.EOF {
				glog.Error(err)
				return nil, false
			}
			if int64(n) != header.Size {
				glog.Errorf("expected to read %d bytes, read %d instead", header.Size, n)
				return nil, false
			}

			t.recordsManager.UpdateRecord(name, ext, buff)
			if t.recordsMetaManager.GetRecord(name).SameMembers(t.recordsManager.GetRecord(name)) {
				sample = core.NewSample(name, t.recordsManager.GetRecord(name).Members)
				t.recordsMetaManager.DeleteRecord(name)
				t.recordsManager.DeleteRecord(name)
				return sample, true
			}
		}
	}
}
