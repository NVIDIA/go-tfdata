// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

package pipeline

import (
	"os"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
	"github.com/NVIDIA/go-tfdata/tfdata/transform/selection"
)

func Example() {
	// read TAR source.tar, convert to TFRecord and save to dest.tfrecord
	input, _ := os.Open("source.tar")
	output, _ := os.Create("dest.tfrecord")
	pipeline := NewPipeline()
	pipeline.FromTar(input).SampleToTFExample().ToTFRecord(output).Do()
}

func Example_typesMapping() {
	input, _ := os.Open("source.tar")
	output, _ := os.Create("dest.tfrecord")

	pipeline := NewPipeline()
	pipeline.FromTar(input).SampleToTFExample(core.TypesMap{
		"jpeg": core.FeatureType.BYTES,
		"cls":  core.FeatureType.INT64,
	}).ToTFRecord(output)
	pipeline.Do()
}

func Example_filterEmptySamples() {
	input, _ := os.Open("source.tar")
	output, _ := os.Create("dest.tfrecord")

	pipeline := NewPipeline()
	pipeline.FromTar(input).FilterEmptySamples().ToTFRecord(output).Do()
}

func Example_selectOnlyImage() {
	input, _ := os.Open("source.tar")
	output, _ := os.Create("dest.tfrecord")

	pipeline := NewPipeline()
	pipeline.FromTar(input).TransformSamples(transform.RenameTransformation("img", []string{"jpeg", "png"}))
	pipeline.TransformSamples(transform.SampleSelections(selection.ByKey("img")))
	pipeline.SampleToTFExample().ToTFRecord(output).Do()
}
