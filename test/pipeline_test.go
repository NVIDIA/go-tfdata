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

	p = pipeline.NewProcess()
	p.FromTar(sourceFd)
	p.WithLocalSamplesTransformations(transform.ID{})
	p.WithDefaultSample2TFExample()
	p.WithLocalTFExamplesTransformations(
		transform.RenameTransformation("image", []string{"jpeg", "jpg"}),
		transform.NewExampleSelections(selection.ByKey("image")))
	p.ToTFRecord(sinkFd)

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
