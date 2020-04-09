package test

import (
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
	return tfReader.Read()
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
	if err != nil {
		t.Error(err)
		return
	}

	f.Close()
	f, err = os.Open(path)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()

	readTfExamples, err := readExamples(f)
	if err != nil {
		t.Error(err)
		return
	}

	if len(readTfExamples) != cnt {
		t.Errorf("expected to read %d examples, but got %d", cnt, len(readTfExamples))
		return
	}

	for i := range tfExamples {
		if !protobuf.Equal(tfExamples[i], readTfExamples[i]) {
			t.Errorf("example %s doesn't equal example %s", tfExamples[i].String(), readTfExamples[i].String())
			return
		}
	}
}
