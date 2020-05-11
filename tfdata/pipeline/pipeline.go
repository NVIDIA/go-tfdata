// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

// Package pipeline provides abstraction of pipeline and stages. It is a basic tool to convert TAR/TAR GZ file into TFRecord file.
package pipeline

import (
	"io"

	"github.com/NVIDIA/go-tfdata/tfdata/archive"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/filter"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
)

type (
	// TarStage produces core.SampleReader
	TarStage func() (core.SampleReader, error)
	// SamplesStage makes transformation on core.Sample: consumes core.SampleReader and produces core.SampleReader
	SamplesStage func(core.SampleReader) core.SampleReader
	// Samples2ExampleStage transforms core.Sample to core.TFExample: consumes core.SampleReader and produces core.TFExampleReader
	Sample2TFExampleStage func(core.SampleReader) core.TFExampleReader
	// SamplesStage makes transformation on core.TFExample: consumes core.TFExampleReader and produces core.TFExampleReader
	TFExamplesStage func(core.TFExampleReader) core.TFExampleReader
	// TFRecordStage consumes core.TFExampleReader
	TFRecordStage func(core.TFExampleReader) error

	// DefaultPipeline represents TAR file to TFRecord file conversion with an intermediate
	// transformations on core.Sample and core.TFExample
	DefaultPipeline struct {
		tarStage            TarStage
		samplesStage        SamplesStage // optional stage - consumes the same type as produces
		sample2ExampleStage Sample2TFExampleStage
		tfExamplesStage     TFExamplesStage // optional stage - consumes the same type as produces
		tfRecordStage       TFRecordStage
	}
)

// TransformTFExamples adds transforming core.Samples according to tfs as pipeline's SamplesStage.
// Transformations will be executed in order of appearance in tfs
func (p *DefaultPipeline) TransformSamples(tfs ...transform.SampleTransformation) *DefaultPipeline {
	return p.WithSamplesStage(func(r core.SampleReader) core.SampleReader {
		return transform.NewSampleTransformer(r, tfs...)
	})
}

// TransformTFExamples adds transforming core.TFExamples according to tfs as pipeline's TFExamplesStage.
// Transformations will be executed in order of appearance in tfs
func (p *DefaultPipeline) TransformTFExamples(tfs ...transform.TFExampleTransformation) *DefaultPipeline {
	return p.WithTFExamplesStage(func(r core.TFExampleReader) core.TFExampleReader {
		return transform.NewTFExampleTransformer(r, tfs...)
	})
}

// FilterEmptySamples adds filtering of empty core.Samples as pipeline's SamplesStage.
func (p *DefaultPipeline) FilterEmptySamples() *DefaultPipeline {
	return p.WithSamplesStage(filter.EmptySamples)
}

// FilterEmptyTFExamples adds filtering of empty core.TFExamples as pipeline's TFExamplesStage.
func (p *DefaultPipeline) FilterEmptyTFExamples() *DefaultPipeline {
	return p.WithTFExamplesStage(filter.EmptyExamples)
}

// FromTar adds reading core.Samples from input as input was a TAR file.
func (p *DefaultPipeline) FromTar(input io.Reader) *DefaultPipeline {
	return p.WithTarStage(func() (core.SampleReader, error) {
		return archive.NewTarReader(input)
	})
}

// FromTarGz adds reading core.Samples from input as input was a TAR GZ file.
func (p *DefaultPipeline) FromTarGz(input io.Reader) *DefaultPipeline {
	return p.WithTarStage(func() (core.SampleReader, error) {
		return archive.NewTarGzReader(input)
	})
}

// Writers TFExamples to specified writer in TFRecord format
// If numWorkers provided, all pipeline transformations will be done
// asynchronously. It assumes that all underlying Readers are async-safe.
// All default Readers, Transformations, Selections are async-safe.
func (p *DefaultPipeline) ToTFRecord(w io.Writer, numWorkers ...int) *DefaultPipeline {
	return p.WithTFRecordStage(func(reader core.TFExampleReader) error {
		writer := core.NewTFRecordWriter(w)
		if len(numWorkers) > 0 {
			return writer.WriteMessagesAsync(reader, numWorkers[0])
		}
		return writer.WriteMessages(reader)
	})
}

// Converts Samples to TFExamples. TypesMap defines what are actual sample types.
// For each (key, mappedType) pair from TypesMap, TFExample will have feature[key] = value, where
// value is sample[key] converted into type mappedType.
// If m is not provided, each entry from value will be converted to BytesList
// If m provided, but sample has key which is not present in TypesMap, value will be converted to BytesList
func (p *DefaultPipeline) SampleToTFExample(m ...core.TypesMap) *DefaultPipeline {
	return p.WithSample2TFExampleStage(func(sr core.SampleReader) core.TFExampleReader {
		return transform.SamplesToTFExample(sr, m...)
	})
}

// Do executes pipeline based on specified stages.
func (p *DefaultPipeline) Do() error {
	// prepare pipeline
	sReader, err := p.tarStage()
	if err != nil {
		return err
	}

	if p.samplesStage != nil {
		sReader = p.samplesStage(sReader)
	}

	exReader := p.sample2ExampleStage(sReader)

	if p.tfExamplesStage != nil {
		exReader = p.tfExamplesStage(exReader)
	}

	// The whole pipeline is ready, start doing the job
	return p.tfRecordStage(exReader)
}

// default setters

func NewPipeline() *DefaultPipeline {
	return &DefaultPipeline{}
}

// WithTarStage defines TarStage of a pipeline. Overrides previous value.
func (p *DefaultPipeline) WithTarStage(stage TarStage) *DefaultPipeline {
	p.tarStage = stage
	return p
}

// WithSamplesStage defines SamplesStage of a pipeline. If SamplesStage has been already set, the resulting
// SamplesStage will chain together transformations in order of setting.
func (p *DefaultPipeline) WithSamplesStage(stage SamplesStage) *DefaultPipeline {
	if p.samplesStage != nil {
		prevStage := p.samplesStage
		p.samplesStage = func(reader core.SampleReader) core.SampleReader {
			return stage(prevStage(reader))
		}
	} else {
		p.samplesStage = stage
	}
	return p
}

// WithSample2TFExampleStage defines Sample2TFExampleStage of a pipeline. Overrides previous value.
func (p *DefaultPipeline) WithSample2TFExampleStage(stage Sample2TFExampleStage) *DefaultPipeline {
	p.sample2ExampleStage = stage
	return p
}

// WithTFExamplesStage defines TFExamplesStage of a pipeline. If TFExamplesStage has been already set, the resulting
// TFExamplesStage will chain together transformations in order of setting.
func (p *DefaultPipeline) WithTFExamplesStage(stage TFExamplesStage) *DefaultPipeline {
	if p.tfExamplesStage != nil {
		prevStage := p.tfExamplesStage
		p.tfExamplesStage = func(reader core.TFExampleReader) core.TFExampleReader {
			return stage(prevStage(reader))
		}
	} else {
		p.tfExamplesStage = stage
	}
	return p
}

// WithTFRecordStage defines TFRecordStage of a pipeline. Overrides previous value.
func (p *DefaultPipeline) WithTFRecordStage(stage TFRecordStage) *DefaultPipeline {
	p.tfRecordStage = stage
	return p
}
