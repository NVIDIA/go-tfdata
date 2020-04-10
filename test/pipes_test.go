//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package test

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/NVIDIA/go-tfdata/test/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata"
	tarp "github.com/tmbdev/tarp/dpipes"
)

func TFExampleSampleTransform(f func(ex *tfdata.TFExample) tarp.Sample) func(tfdata.TFExamplePipe, tarp.Pipe) {
	return func(inch tfdata.TFExamplePipe, out tarp.Pipe) {
		for ex := range inch {
			out <- f(ex)
		}
		close(out)
	}
}

func SampleToTFExampleTransform(f func(sample tarp.Sample) *tfdata.TFExample) func(tarp.Pipe, tfdata.TFExamplePipe) {
	return func(inch tarp.Pipe, out tfdata.TFExamplePipe) {
		for sample := range inch {
			out <- f(sample)
		}
		close(out)
	}
}

// We know the structure of TFExample so it's hardcoded for this exact TFRecord
func TFExampleToSample(ex *tfdata.TFExample) tarp.Sample {
	sample := make(tarp.Sample)
	label := make([]byte, 8)

	features := ex.GetFeatures().GetFeature()
	sample["image_raw"] = features["image_raw"].GetBytesList().Value[0]
	binary.LittleEndian.PutUint64(label, uint64(features["label"].GetInt64List().Value[0]))
	sample["label"] = label

	return sample
}

func SampleToTFExample(sample tarp.Sample) *tfdata.TFExample {
	ex := tfdata.NewTFExample()
	ex.AddInt64("label", int64(binary.LittleEndian.Uint64(sample["label"])))
	ex.AddBytes("image_raw", sample["image_raw"])
	return ex
}

// Create github.com/tmbdev/tarp/dpipes Pipeline
// Read tf.Record to stream of examples, transform examples to tarp.Samples, shuffle, transform back to TFExamples,
// and write to TFRecord
func TestPipeline(t *testing.T) {
	const (
		sourcePath  = "data/tf-train-medium.record"
		destPath    = "/tmp/tf-train-medium.record"
		examplesCnt = 7
	)

	sourceFd, err := os.Open(sourcePath)
	tassert.CheckFatal(t, err)

	source := func(pipe tarp.Pipe) {
		exCh := make(chan *tfdata.TFExample, 100)
		go tfdata.TFRecordSource(sourceFd)(exCh)
		go TFExampleSampleTransform(TFExampleToSample)(exCh, pipe)
	}

	sinkFd, err := os.Create(destPath)
	tassert.CheckFatal(t, err)
	defer os.Remove(destPath)

	sink := func(pipe tarp.Pipe) {
		exCh := make(chan *tfdata.TFExample, 100)
		go SampleToTFExampleTransform(SampleToTFExample)(pipe, exCh)
		// Do not `go` as we what tarp.Processing to finish when TFRecordSink finishes.
		// Only then, after everything is done, start checking the contents of destPath.
		tfdata.TFRecordSink(sinkFd)(exCh)
	}

	tarp.Processing(source, tarp.Shuffle(7, 7), sink)

	resultFd, err := os.Open(destPath)
	r := tfdata.NewTFRecordReader(resultFd)
	ex, err := r.ReadAllExamples(examplesCnt)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(ex) == examplesCnt, "expected %d tf.Examples, got %d", examplesCnt, len(ex))
	resultFd.Close()
}
