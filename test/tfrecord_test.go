//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package test

import (
	"bytes"
	"image/jpeg"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/NVIDIA/go-tfdata/test/tassert"
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

func writeExamplesCh(w io.Writer, examples []*tfdata.TFExample) error {
	var (
		tfWriter = tfdata.NewTFRecordWriter(w)
		ch       = make(chan *tfdata.TFExample, 5)
		wg       = sync.WaitGroup{}
		err      error
	)

	wg.Add(1)
	go func() {
		err = tfWriter.WriteExamples(ch)
		wg.Done()
	}()

	for _, example := range examples {
		ch <- example
	}
	close(ch)

	wg.Wait()
	return err
}

func readExamples(r io.Reader) ([]*tfdata.TFExample, error) {
	tfReader := tfdata.NewTFRecordReader(r)
	return tfReader.ReadAllExamples()
}

func readExamplesCh(r io.Reader) ([]*tfdata.TFExample, error) {
	var (
		err      error
		tfReader = tfdata.NewTFRecordReader(r)
		wg       = sync.WaitGroup{}
		result   = make([]*tfdata.TFExample, 0, 20)
		ch       = make(chan *tfdata.TFExample, 20)
	)

	wg.Add(1)
	go func() {
		err = tfReader.ReadExamples(ch)
		wg.Done()
	}()

	for ex := range ch {
		result = append(result, ex)
	}

	wg.Wait()
	return result, err
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

// Read TFRecord with single tf.Example
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

// Read TFRecord with multiple tf.Examples
func TestTfMediumRecordReader(t *testing.T) {
	const path = "data/tf-train-medium.record"

	f, err := os.Open(path)
	tassert.CheckFatal(t, err)
	defer f.Close()

	readTfExamples, err := readExamples(f)
	tassert.CheckError(t, err)
	tassert.Errorf(t, len(readTfExamples) == 7, "expected to read 7 tf.Examples, got %d", len(readTfExamples))

	for _, example := range readTfExamples {
		imgFeature := example.Features.Feature["image_raw"]
		value := imgFeature.GetBytesList().GetValue()
		tassert.Errorf(t, len(value) == 1, "expected one element list, got %d elements", len(value))
		img, err := jpeg.Decode(bytes.NewBuffer(value[0]))
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, img.Bounds().Dx() == img.Bounds().Dy() || img.Bounds().Dx() != 224, "unexpected dimensions of an image; expected 224,224")
	}
}

// Create TFRecord and then read it back to memory
func TestTfRecordWriterReader(t *testing.T) {
	const (
		cnt  = 100
		path = "/tmp/testtfrecordwriterreader"
	)
	var (
		writers = []func(w io.Writer, examples []*tfdata.TFExample) error{writeExamples, writeExamplesCh}
		readers = []func(r io.Reader) ([]*tfdata.TFExample, error){readExamples, readExamplesCh}
	)

	defer func() {
		if _, err := os.Stat(path); err == nil {
			os.Remove(path)
		}
	}()

	for _, write := range writers {
		for _, read := range readers {
			f, err := os.Create(path)
			if err != nil {
				t.Error(err)
				return
			}

			tfExamples := prepareExamples(cnt)
			err = write(f, tfExamples)
			tassert.CheckError(t, err)

			f.Close()
			f, err = os.Open(path)
			tassert.CheckFatal(t, err)

			readTfExamples, err := read(f)
			tassert.CheckError(t, err)

			tassert.Errorf(t, len(readTfExamples) == cnt, "expected to read %d examples, but got %d", cnt, len(readTfExamples))

			for i := range tfExamples {
				tassert.Errorf(t, protobuf.Equal(tfExamples[i], readTfExamples[i]), "example %s doesn't equal example %s", tfExamples[i].String(), readTfExamples[i].String())
			}
			f.Close()
			os.Remove(path)
		}
	}
}
