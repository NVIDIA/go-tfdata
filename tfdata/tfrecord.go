//// Package tfdata provides interface to interact with TFRecord files and tf.Examples
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package tfdata

import (
	"encoding/binary"
	"io"

	"github.com/NVIDIA/go-tfdata/tfdata/internal/checksum"
	protobuf "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type (
	TFRecordWriterInterface interface {
		io.Writer
		WriteMessage(protobuf.Message) (n int, err error)
		WriteExample(*TFExample) (n int, err error)

		WriteMessages(fromCh protobuf.Message) error
	}

	TFRecordReaderInterface interface {
		ReadNext(protobuf.Message) error
		ReadNextExample() (*TFExample, error)

		ReadAllExamples(expectedSize ...int) ([]*TFExample, error)
		ReadExamples(toCh chan *TFExample) error
	}

	TFRecordWriter struct {
		w io.Writer
		c checksum.Checksummer
	}

	TFRecordReader struct {
		r io.Reader
		c checksum.Checksummer
	}
)

func NewTFRecordWriter(w io.Writer) *TFRecordWriter {
	return &TFRecordWriter{w: w, c: checksum.NewCRCChecksummer()}
}

// https://www.tensorflow.org/tutorials/load_data/tfrecord#tfrecords_format_details
func (w *TFRecordWriter) Write(p []byte) (n int, err error) {
	var (
		total         = 0
		lengthHeader  = make([]byte, 12) // uint64(length) + uint32 cksm
		dataCksmBytes = make([]byte, 4)  // uint32 cksm

	)

	binary.LittleEndian.PutUint64(lengthHeader[:8], uint64(len(p)))
	binary.LittleEndian.PutUint32(lengthHeader[8:12], w.c.Get(lengthHeader[:8]))
	binary.LittleEndian.PutUint32(dataCksmBytes, w.c.Get(p))

	read, err := w.w.Write(lengthHeader)
	total += read
	if err == nil {
		read, err = w.w.Write(p)
		total += read
	}
	if err == nil {
		read, err = w.w.Write(dataCksmBytes)
		total += read
	}

	return total, err
}

func (w *TFRecordWriter) WriteExample(example *TFExample) (n int, err error) {
	return w.WriteMessage(example)
}

func (w *TFRecordWriter) WriteMessage(message protoreflect.ProtoMessage) (n int, err error) {
	p, err := protobuf.Marshal(message)
	if err != nil {
		return 0, err
	}

	return w.Write(p)
}

func (w *TFRecordWriter) WriteMessages(ch <-chan protoreflect.ProtoMessage) error {
	for message := range ch {
		_, err := w.WriteMessage(message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *TFRecordWriter) WriteExamples(ch <-chan *TFExample) error {
	for message := range ch {
		_, err := w.WriteExample(message)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewTFRecordReader(r io.Reader) *TFRecordReader {
	return &TFRecordReader{r: r, c: checksum.NewCRCChecksummer()}
}

func (r *TFRecordReader) ReadNextExample() (*TFExample, error) {
	ex := &TFExample{}
	return ex, r.ReadNext(ex)
}

func (r *TFRecordReader) ReadNext(message protobuf.Message) error {
	payloadLengthHeader := make([]byte, 12)
	if _, err := io.ReadFull(r.r, payloadLengthHeader); err != nil {
		return err
	}

	lengthChecksum := binary.LittleEndian.Uint32(payloadLengthHeader[8:12])
	if err := r.c.Verify(payloadLengthHeader[:8], lengthChecksum); err != nil {
		return err
	}

	payloadLength := binary.LittleEndian.Uint64(payloadLengthHeader[0:8])
	payload := make([]byte, payloadLength)
	if _, err := io.ReadFull(r.r, payload); err != nil {
		return err
	}

	payloadChecksumBytes := make([]byte, 4)
	if _, err := io.ReadFull(r.r, payloadChecksumBytes); err != nil {
		return err
	}

	payloadChecksum := binary.LittleEndian.Uint32(payloadChecksumBytes[0:4])
	if err := r.c.Verify(payload, payloadChecksum); err != nil {
		return err
	}

	// TODO: think how we should unmarshal message based on given MessageDescriptor
	return protobuf.Unmarshal(payload, message)
}

func (r *TFRecordReader) ReadAllExamples(expectedSize ...int) ([]*TFExample, error) {
	expectedExamplesCnt := 20
	if len(expectedSize) > 0 {
		expectedExamplesCnt = expectedSize[0]
	}
	result := make([]*TFExample, 0, expectedExamplesCnt)

	for {
		ex, err := r.ReadNextExample()
		if err == nil {
			result = append(result, ex)
		} else if err == io.EOF {
			break
		} else {
			return nil, err
		}
	}

	return result, nil
}

func (r *TFRecordReader) ReadExamples(ch chan<- *TFExample) error {
	defer close(ch)
	for {
		ex, err := r.ReadNextExample()
		if err == nil {
			ch <- ex
		} else if err == io.EOF {
			break
		} else {
			return err
		}
	}

	return nil
}

func TFRecordSource(stream io.ReadCloser) func(pipe TFExamplePipe) {
	return func(outch TFExamplePipe) {
		r := NewTFRecordReader(stream)
		r.ReadExamples(outch)
		_ = stream.Close()
	}
}

func TFRecordSink(dest io.WriteCloser) func(pipe TFExamplePipe) {
	return func(inch TFExamplePipe) {
		w := NewTFRecordWriter(dest)
		w.WriteExamples(inch)
		_ = dest.Close()
	}
}
