//// Package tfdata provides interface to interact with TFRecord files and TExamples
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package core

import (
	"encoding/binary"
	"io"

	"github.com/NVIDIA/go-tfdata/tfdata/internal/checksum"
	protobuf "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type (
	// TFRecordWriterInterface is an interface which writes objects in TFRecord format.
	// TFRecord format is described here: https://www.tensorflow.org/tutorials/load_data/tfrecord#tfrecords_format_details
	TFRecordWriterInterface interface {
		io.Writer
		WriteMessage(protobuf.Message) (n int, err error)
		WriteExample(*TFExample) (n int, err error)

		WriteMessages(reader TFExampleReader) error
	}

	// TFRecordReaderInterface is an interface which reads objects from TFRecord format
	// TFRecord format is described here: https://www.tensorflow.org/tutorials/load_data/tfrecord#tfrecords_format_details
	TFRecordReaderInterface interface {
		ReadNext(protobuf.Message) error
		ReadNextExample() (*TFExample, error)

		ReadAllExamples(expectedSize ...int) ([]*TFExample, error)
		ReadExamples(writer TFExampleWriter) error
	}

	// TFRecordWriter implements TFRecordWriter interface
	// It writes objects into writer w with checksums provided by c
	TFRecordWriter struct {
		w io.Writer
		c checksum.Checksummer
	}

	// TFRecordReader implements TFRecordReader interface
	// It reads objects from reader r and verify checksums with c
	TFRecordReader struct {
		r io.Reader
		c checksum.Checksummer
	}
)

// NewTFRecordWriter creates and initializes TFRecordWriter with writer w and CRC checksumming method.
// Returns pointer to created TFRecordWriter
func NewTFRecordWriter(w io.Writer) *TFRecordWriter {
	return &TFRecordWriter{w: w, c: checksum.NewCRCChecksummer()}
}

// Write writes p into writer into format specified in https://www.tensorflow.org/tutorials/load_data/tfrecord#tfrecords_format_details.
// If any of underlying writes to internal writer fails, number or already written bytes and error is returned
// Write is not atomic, meaning that underlying write error might leave internal writer in invalid TFRecord state
// Returns total number of written bytes and error if occurred
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

// WriteMessage marshals message and writes it to TFRecord
func (w *TFRecordWriter) WriteMessage(message protoreflect.ProtoMessage) (n int, err error) {
	p, err := protobuf.Marshal(message)
	if err != nil {
		return 0, err
	}

	return w.Write(p)
}

// WriteMessages reads and writes to TFRecord messages one-by-one from ch and terminates
// when ch is closed and empty. WriteMessages doesn't close ch itself.
// Returns error immediately if occurred, without processing subsequent messages
func (w *TFRecordWriter) WriteMessages(reader TFExampleReader) error {
	for ex, ok := reader.Read(); ok; ex, ok = reader.Read() {
		_, err := w.WriteMessage(ex)
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteExamples behaves the same as WriteMessages but operates on channel of TFExamples
func (w *TFRecordWriter) WriteExamples(ch <-chan *TFExample) error {
	for message := range ch {
		_, err := w.WriteExample(message)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewTFRecordReader creates and initializes TFRecordReader with writer w and CRC checksumming method.
// Returns pointer to created TFRecordReader
func NewTFRecordReader(r io.Reader) *TFRecordReader {
	return &TFRecordReader{r: r, c: checksum.NewCRCChecksummer()}
}

func (r *TFRecordReader) ReadNextExample() (*TFExample, error) {
	ex := &TFExample{}
	return ex, r.ReadNext(ex)
}

// ReadNext reads next message from reader and stores it in provided message
// If error occurred ReadNext terminates immediately.
// If read bytes are not in TFRecord format ReadNext terminates with error.
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

// ReadAllExamples reads examples from TFRecord until EOF and loads them into memory
// If error occurred it terminates immediately without reading subsequent samples
// Returns slice of examples and error if occurred.
func (r *TFRecordReader) ReadAllExamples(expectedSize ...int) ([]*TFExample, error) {
	expectedExamplesCnt := 20
	if len(expectedSize) > 0 {
		expectedExamplesCnt = expectedSize[0]
	}
	result := make([]*TFExample, 0, expectedExamplesCnt)

	for {
		ex, err := r.ReadNextExample()
		switch err {
		case nil:
			result = append(result, ex)
		case io.EOF:
			return result, nil
		default:
			return nil, err
		}
	}
}

// ReadExamples reads and puts into ch examples from TFRecord one-by-one
// It error occurred, ReadExamples terminates immediately, without processing subsequent samples.
// ReadExamples closes ch upon termination.
func (r *TFRecordReader) ReadExamples(writer TFExampleWriter) error {
	defer writer.Close()
	for {
		ex, err := r.ReadNextExample()
		switch err {
		case nil:
			err = writer.Write(ex)
			if err != nil {
				return err
			}
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}
