//// Package test contains tests of tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package test

import (
	"os"
	"testing"

	"github.com/NVIDIA/go-tfdata/test/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/pipeline"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
	"github.com/NVIDIA/go-tfdata/tfdata/transform/selection"
)

func TestSmallPipeline(t *testing.T) {
	const (
		sourcePath  = "data/small-10.tar"
		destPath    = "/tmp/small-10.record"
		examplesCnt = 10
	)
	var (
		sourceFd, sinkFd *os.File
		err              error
		p                *pipeline.DefaultPipeline
		examples         []*core.TFExample
	)

	sourceFd, err = os.Open(sourcePath)
	tassert.CheckFatal(t, err)
	sinkFd, err = os.Create(destPath)
	tassert.CheckFatal(t, err)
	defer os.Remove(destPath)

	p = pipeline.NewPipeline()
	// declare that Samples should be read from sourceFd
	p.FromTar(sourceFd)
	// declare default sample to tfExample transformation
	p.DefaultSampleToTFExample()
	// declare that TFExamples should be written to sinkFd in TFRecord format
	p.ToTFRecord(sinkFd)
	// execute the pipeline
	p.Do()

	sourceFd.Close()
	sinkFd.Close()

	sinkFd, err = os.Open(destPath)
	tassert.CheckFatal(t, err)
	w := core.NewTFRecordReader(sinkFd)
	examples, err = w.ReadAllExamples(examplesCnt)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(examples) == examplesCnt, "expected to read %d examples, but got %d", examplesCnt, len(examples))
}

func TestPipeline(t *testing.T) {
	const (
		sourcePath  = "data/small-10.tar"
		destPath    = "/tmp/small-10.record"
		examplesCnt = 10
	)
	var (
		sourceFd, sinkFd *os.File
		err              error
		p                *pipeline.DefaultPipeline
		examples         []*core.TFExample
	)

	sourceFd, err = os.Open(sourcePath)
	tassert.CheckFatal(t, err)
	sinkFd, err = os.Create(destPath)
	tassert.CheckFatal(t, err)
	defer os.Remove(destPath)

	p = pipeline.NewPipeline()
	// declare that Samples should be read from sourceFd
	p.FromTar(sourceFd)
	// declare Samples transformation - ID - does nothing
	p.TransformSamples(transform.ID{})
	// declare default sample to tfExample transformation
	p.DefaultSampleToTFExample()
	// declare TFExample transformations
	p.TransformTFExamples(
		// rename fields "jpeg" and "jpg" to "image"
		transform.RenameTransformation("image", []string{"jpeg", "jpg"}),
		// select only "image" entry from TFExample
		transform.ExampleSelections(selection.ByKey("image")))
	// filter empty Examples: those which didn't have "image" entry
	p.FilterEmptyExamples()
	// write Examples to sinkFd in TFRecord format
	p.ToTFRecord(sinkFd)
	// execute the pipeline
	p.Do()

	sourceFd.Close()
	sinkFd.Close()

	sinkFd, err = os.Open(destPath)
	tassert.CheckFatal(t, err)
	w := core.NewTFRecordReader(sinkFd)
	examples, err = w.ReadAllExamples(examplesCnt)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(examples) == examplesCnt, "expected to read %d examples, but got %d", examplesCnt, len(examples))
}
