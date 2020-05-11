// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

//Package transform provides implementation of tfdata.Transformation
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
		TransformSample(s core.Sample) core.Sample
	}

	// Sample transformation based on selections. Result is union of selections
	SampleSelectionsTransformation struct {
		selections []selection.Sample
	}

	// Example transformation based on selections. Result is union of selections
	ExampleSelectionsTransformation struct {
		selections []selection.Example
	}

	Rename struct {
		dest string
		src  []string
	}

	// ID transformation, does nothing
	ID struct{}

	// Transformation based on function
	SampleFuncTransformation struct {
		f func(core.Sample) core.Sample
	}

	// Transformation based on function
	TFExampleFuncTransformation struct {
		f func(example *core.TFExample) *core.TFExample
	}
)

var (
	_, _, _ SampleTransformation    = ID{}, &Rename{}, &SampleSelectionsTransformation{}
	_, _, _ TFExampleTransformation = ID{}, &Rename{}, &ExampleSelectionsTransformation{}
)

func RenameTransformation(dest string, src []string) *Rename {
	return &Rename{src: src, dest: dest}
}

func (c *Rename) TransformSample(sample core.Sample) core.Sample {
	for _, src := range c.src {
		if val, ok := sample[src]; ok {
			sample[c.dest] = val
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

func (t ID) TransformSample(s core.Sample) core.Sample {
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

func (s *SampleSelectionsTransformation) TransformSample(sample core.Sample) core.Sample {
	keysSubset := make(map[string]struct{})
	for _, selection := range s.selections {
		for _, key := range selection.SelectSample(sample) {
			keysSubset[key] = struct{}{}
		}
	}

	for k := range sample {
		if _, ok := keysSubset[k]; !ok {
			delete(sample, k)
		}
	}
	return sample
}

// Return Transformation based on specified Selections. Resulting Samples are unions of selections.
func SampleSelections(s ...selection.Sample) *SampleSelectionsTransformation {
	return &SampleSelectionsTransformation{selections: s}
}

// Return Transformation based on specified Selections. Resulting TFExamples are unions of selections.
func ExampleSelections(s ...selection.Example) *ExampleSelectionsTransformation {
	return &ExampleSelectionsTransformation{selections: s}
}

func SampleF(f func(core.Sample) core.Sample) *SampleFuncTransformation {
	return &SampleFuncTransformation{f: f}
}

func (t *SampleFuncTransformation) TransformSample(sample core.Sample) core.Sample {
	return t.f(sample)
}

func ExampleF(f func(*core.TFExample) *core.TFExample) *TFExampleFuncTransformation {
	return &TFExampleFuncTransformation{f: f}
}

func (t *TFExampleFuncTransformation) TransformTFExample(ex *core.TFExample) *core.TFExample {
	return t.f(ex)
}
