//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package test

import (
	"bytes"
	"github.com/NVIDIA/go-tfdata/test/tassert"
	"image/jpeg"
	"io"
	"os"
	"testing"

	"github.com/NVIDIA/go-tfdata/tfdata"
	protobuf "google.golang.org/protobuf/proto"
)

func writeExamples(w io.Writer, examples []*tfdata.TFExample) error {
	tfWriter := tfdata.NewTFRecordWriter(w)
	for _, example := range examples {
		_, err := tfWriter.WriteExample(example)
		if err != nil {
			return err
		}
	}
	return nil
}

func readExamples(r io.Reader) ([]*tfdata.TFExample, error) {
	tfReader := tfdata.NewTFRecordReader(r)
	return tfReader.ReadExamples()
}

func prepareExamples(cnt int) []*tfdata.TFExample {
	result := make([]*tfdata.TFExample, 0, cnt)
	for i := 0; i < cnt; i++ {
		ex := tfdata.NewTFExample()
		ex.AddIntList("int-list", []int{0, 1, 2, 3, 4, 5})
		ex.AddFloat("float", 0.42)
		ex.AddBytes("bytes", []byte("bytesstring"))
		result = append(result, ex)
	}

	return result
}

func TestSmokeTfSingleRecordReader(t *testing.T) {
	const path = "data/tf-train-single.record"

	f, err := os.Open(path)
	tassert.CheckFatal(t, err)
	defer f.Close()

	readTfExamples, err := readExamples(f)
	tassert.CheckError(t, err)

	if len(readTfExamples) != 1 {
		t.Errorf("expected to read one tf.Examples, got %d", len(readTfExamples))
	}
}

func TestTfMediumRecordReader(t *testing.T) {
	const path = "data/tf-train-medium.record"

	f, err := os.Open(path)
	tassert.CheckFatal(t, err)
	defer f.Close()

	readTfExamples, err := readExamples(f)
	tassert.CheckError(t, err)
	tassert.Errorf(t, len(readTfExamples) == 7, "expected to read 7 tf.Examples, got %d", len(readTfExamples))

	for _, example := range readTfExamples {
		s := ""
		for k := range example.Features.Feature {
			s += k + " "
		}

		imgFeature := example.Features.Feature["image_raw"]
		value := imgFeature.GetBytesList().GetValue()
		tassert.Errorf(t, len(value) == 1, "expected one element list, got %d elements", len(value))
		img, err := jpeg.Decode(bytes.NewBuffer(value[0]))
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, img.Bounds().Dx() == img.Bounds().Dy() || img.Bounds().Dx() != 224, "unexpected dimensions of an image; expected 224,224")
	}
}

func TestTfRecordWriterReader(t *testing.T) {
	const (
		cnt  = 100
		path = "/tmp/testtfrecordwriterreader"
	)
	f, err := os.Create(path)
	if err != nil {
		t.Error(err)
		return
	}
	defer os.Remove(path)

	tfExamples := prepareExamples(cnt)
	err = writeExamples(f, tfExamples)
	tassert.CheckError(t, err)

	f.Close()
	f, err = os.Open(path)
	tassert.CheckFatal(t, err)
	defer f.Close()

	readTfExamples, err := readExamples(f)
	tassert.CheckError(t, err)

	tassert.Errorf(t, len(readTfExamples) == cnt, "expected to read %d examples, but got %d", cnt, len(readTfExamples))

	for i := range tfExamples {
		tassert.Errorf(t, protobuf.Equal(tfExamples[i], readTfExamples[i]), "example %s doesn't equal example %s", tfExamples[i].String(), readTfExamples[i].String())
	}
}
