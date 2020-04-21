//// Package transform provides implementation of tfdata.Transformation
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package transform

import (
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/transform/selection"
)

type (
	TFExampleTransformation interface {
		TransformTFExample(ex *core.TFExample) *core.TFExample
	}

	SampleTransformation interface {
		TransformSample(s *core.Sample) *core.Sample
	}

	SampleSelectionsTransformation struct {
		selections []selection.Sample
	}

	ExampleSelectionsTransformation struct {
		selections []selection.Example
	}

	Rename struct {
		dest string
		src  []string
	}

	ID struct{}
)

var (
	_, _, _ SampleTransformation    = ID{}, &Rename{}, &SampleSelectionsTransformation{}
	_, _, _ TFExampleTransformation = ID{}, &Rename{}, &ExampleSelectionsTransformation{}
)

func RenameTransformation(dest string, src []string) *Rename {
	return &Rename{src: src, dest: dest}
}

func (c *Rename) TransformSample(sample *core.Sample) *core.Sample {
	for _, src := range c.src {
		if val, ok := sample.Entries[src]; ok {
			sample.Entries[c.dest] = val
		}
	}

	return sample
}

func (c *Rename) TransformTFExample(ex *core.TFExample) *core.TFExample {
	for _, src := range c.src {
		if ex.HasFeature(src) {
			ex.SetFeature(c.dest, ex.GetFeature(src))
		}
	}

	return ex
}

func (t ID) TransformTFExample(ex *core.TFExample) *core.TFExample {
	return ex
}

func (t ID) TransformSample(s *core.Sample) *core.Sample {
	return s
}

func (s *ExampleSelectionsTransformation) TransformTFExample(ex *core.TFExample) *core.TFExample {
	keysSubset := make(map[string]struct{})
	for _, selection := range s.selections {
		for _, key := range selection.SelectExample(ex) {
			keysSubset[key] = struct{}{}
		}
	}

	for k := range ex.GetFeatures().Feature {
		if _, ok := keysSubset[k]; !ok {
			delete(ex.GetFeatures().Feature, k)
		}
	}
	return ex
}

func (s *SampleSelectionsTransformation) TransformSample(sample *core.Sample) *core.Sample {
	keysSubset := make(map[string]struct{})
	for _, selection := range s.selections {
		for _, key := range selection.SelectSample(sample) {
			keysSubset[key] = struct{}{}
		}
	}

	for k := range sample.Entries {
		if _, ok := keysSubset[k]; !ok {
			delete(sample.Entries, k)
		}
	}
	return sample
}

// Return Transformation based on specified Selections
func SampleSelections(s ...selection.Sample) *SampleSelectionsTransformation {
	return &SampleSelectionsTransformation{selections: s}
}

// return Transformation based on specified Selections
func ExampleSelections(s ...selection.Example) *ExampleSelectionsTransformation {
	return &ExampleSelectionsTransformation{selections: s}
}
