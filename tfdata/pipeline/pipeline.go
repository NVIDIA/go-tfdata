//// Package pipeline provides abstraction of pipeline and stages
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package pipeline

import (
	"io"

	"github.com/NVIDIA/go-tfdata/tfdata/archive"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/filter"
	"github.com/NVIDIA/go-tfdata/tfdata/internal/cmn"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
)

type (
	TarStage            func() core.SampleReader
	SamplesStage        func(core.SampleReader) core.SampleReader
	Sample2ExampleStage func(core.SampleReader) core.TFExampleReader
	TFExamplesStage     func(core.TFExampleReader) core.TFExampleReader
	TFRecordStage       func(core.TFExampleReader)

	DefaultPipeline struct {
		tarStage            TarStage
		samplesStage        SamplesStage // optional stage - consumes the same type as produces
		sample2ExampleStage Sample2ExampleStage
		tfExamplesStage     TFExamplesStage // optional stage - consumes the same type as produces
		tfRecordStage       TFRecordStage
	}
)

func (p *DefaultPipeline) TransformSamples(tfs ...transform.SampleTransformation) *DefaultPipeline {
	cmn.Assert(p.samplesStage == nil)
	return p.WithSamplesStage(func(r core.SampleReader) core.SampleReader {
		return transform.NewSampleTransformer(r, tfs...)
	})
}

func (p *DefaultPipeline) TransformTFExamples(tfs ...transform.TFExampleTransformation) *DefaultPipeline {
	cmn.Assert(p.tfExamplesStage == nil)
	return p.WithTFExamplesStage(func(r core.TFExampleReader) core.TFExampleReader {
		return transform.NewTFExampleTransformer(r, tfs...)
	})
}

func (p *DefaultPipeline) FilterEmptySamples() *DefaultPipeline {
	samplesStage := p.samplesStage
	return p.WithSamplesStage(func(reader core.SampleReader) core.SampleReader {
		if samplesStage != nil {
			reader = samplesStage(reader)
		}
		return filter.EmptySamples(reader)
	})
}

func (p *DefaultPipeline) FilterEmptyExamples() *DefaultPipeline {
	tfExamplesStage := p.tfExamplesStage
	return p.WithTFExamplesStage(func(reader core.TFExampleReader) core.TFExampleReader {
		if tfExamplesStage != nil {
			reader = tfExamplesStage(reader)
		}
		return filter.EmptyExamples(reader)
	})
}

func (p *DefaultPipeline) FromTar(input io.Reader) *DefaultPipeline {
	cmn.Assert(p.tarStage == nil)
	return p.WithTarStage(func() core.SampleReader {
		sampleReader, err := archive.NewTarReader(input)
		cmn.Assert(err == nil)
		return sampleReader
	})
}

func (p *DefaultPipeline) FromTarGz(input io.Reader) *DefaultPipeline {
	cmn.Assert(p.tarStage == nil)
	return p.WithTarStage(func() core.SampleReader {
		sampleReader, err := archive.NewTarGzReader(input)
		cmn.Assert(err == nil)
		return sampleReader
	})
}

// Writers TFExamples to specified writer in TFRecord format
// If numWorkers provided, all pipeline transformations will be done
// asynchronously. It assumes that all underlying Readers are async-safe.
// All default Readers, Transformations, Selections are async-safe.
func (p *DefaultPipeline) ToTFRecord(w io.Writer, numWorkers ...int) *DefaultPipeline {
	cmn.Assert(p.tfRecordStage == nil)
	return p.WithTFRecordStage(func(reader core.TFExampleReader) {
		writer := core.NewTFRecordWriter(w)
		var err error
		if len(numWorkers) > 0 {
			err = writer.WriteMessagesAsync(reader, numWorkers[0])
		} else {
			err = writer.WriteMessages(reader)
		}
		cmn.Assert(err == nil)
	})
}

func (p *DefaultPipeline) DefaultSampleToTFExample() *DefaultPipeline {
	cmn.Assert(p.sample2ExampleStage == nil)
	return p.WithSample2ExampleStage(func(sr core.SampleReader) core.TFExampleReader {
		return transform.NewSamplesToTFExample(sr)
	})
}

func (p *DefaultPipeline) Do() {
	// prepare pipeline
	sReader := p.tarStage()
	if p.samplesStage != nil {
		sReader = p.samplesStage(sReader)
	}
	exReader := p.sample2ExampleStage(sReader)
	if p.tfExamplesStage != nil {
		exReader = p.tfExamplesStage(exReader)
	}

	// The whole pipeline is ready, start doing the job
	p.tfRecordStage(exReader)
}

// default setters

func NewPipeline() *DefaultPipeline {
	return &DefaultPipeline{}
}

func (p *DefaultPipeline) WithTarStage(stage TarStage) *DefaultPipeline {
	p.tarStage = stage
	return p
}

func (p *DefaultPipeline) WithSamplesStage(stage SamplesStage) *DefaultPipeline {
	p.samplesStage = stage
	return p
}

func (p *DefaultPipeline) WithSample2ExampleStage(stage Sample2ExampleStage) *DefaultPipeline {
	p.sample2ExampleStage = stage
	return p
}

func (p *DefaultPipeline) WithTFExamplesStage(stage TFExamplesStage) *DefaultPipeline {
	p.tfExamplesStage = stage
	return p
}

func (p *DefaultPipeline) WithTFRecordStage(stage TFRecordStage) *DefaultPipeline {
	p.tfRecordStage = stage
	return p
}
