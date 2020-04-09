//// Package tfdata provides interface to interact with TFRecord files and tf.Examples
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package tfdata

import (
	"encoding/binary"
	protobuf "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"io"

	"github.com/NVIDIA/go-tfdata/tfdata/internal/checksum"
)

type (
	TFRecordWriterInterface interface {
		io.Writer
		WriteMessage(protobuf.Message) (n int, err error)
		WriteExample(*TFExample) (n int, err error)
	}

	TFRecordReaderInterface interface {
		ReadNext(protobuf.Message) error
		ReadNextExample() (*TFExample, error)

		ReadExamples() ([]*TFExample, error)
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

func (r *TFRecordReader) ReadExamples() ([]*TFExample, error) {
	result := make([]*TFExample, 0, 20)

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
