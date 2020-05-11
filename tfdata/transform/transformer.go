// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

package transform

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/internal/cmn"
	jsoniter "github.com/json-iterator/go"
)

type (
	// TFExampleTransformer - it does everything locally, doesn't even preload anything from internal reader.
	// However, it's easy to imagine more advanced Transformer (Reader) which has the same Read() interface,
	// but underneath prefetches TFExamples, distributes them amongst external workers
	// and after transformations gathers TFExamples and make them available to Read()
	TFExampleTransformer struct {
		reader          core.TFExampleReader
		transformations []TFExampleTransformation
	}

	// Transforms SamplesReader based on given transformations
	SamplesTransformer struct {
		reader          core.SampleReader
		transformations []SampleTransformation
	}

	// Default SamplesToTFExamples transformer: put into TFExample each of Sample entries as BytesList
	SamplesToTFExamplesTransformer struct {
		reader core.SampleReader
	}

	SampleToTFExamplesTypesTransformer struct {
		reader   core.SampleReader
		typesMap map[string]core.TFFeatureType
	}
)

var (
	_, _ core.TFExampleReader = &TFExampleTransformer{}, &SamplesToTFExamplesTransformer{}
	_    core.SampleReader    = &SamplesTransformer{}
)

// NewTFExampleTransformer consumes TFExampleReader, applies transformations in order of occurrence, produces TFExampleReader.
func NewTFExampleTransformer(reader core.TFExampleReader, ts ...TFExampleTransformation) core.TFExampleReader {
	return &TFExampleTransformer{
		reader:          reader,
		transformations: ts,
	}
}

func (t *TFExampleTransformer) Read() (*core.TFExample, error) {
	ex, err := t.reader.Read()
	if err != nil {
		return nil, err
	}
	for _, t := range t.transformations {
		ex = t.TransformTFExample(ex)
	}
	return ex, nil
}

// NewSampleTransformer consumes TFExampleReader, applies transformations in order of occurrence, produces SampleReader.
func NewSampleTransformer(reader core.SampleReader, ts ...SampleTransformation) core.SampleReader {
	return &SamplesTransformer{
		reader:          reader,
		transformations: ts,
	}
}

func (t *SamplesTransformer) Read() (core.Sample, error) {
	sample, err := t.reader.Read()
	if err != nil {
		return nil, err
	}
	for _, t := range t.transformations {
		sample = t.TransformSample(sample)
	}
	return sample, nil
}

// SamplesToTFExample consumes SampleReader, applies default Sample to TFExample conversion, produces TFExampleReader.
// Default Sample to TFExample conversion is put into TFExample each of Sample entries as BytesList
func SamplesToTFExample(reader core.SampleReader, types ...core.TypesMap) core.TFExampleReader {
	if len(types) == 0 {
		return &SamplesToTFExamplesTransformer{reader: reader}
	}
	cmn.Assert(len(types) == 1)
	return &SampleToTFExamplesTypesTransformer{reader: reader, typesMap: types[0]}
}

func (t *SamplesToTFExamplesTransformer) Read() (*core.TFExample, error) {
	var (
		b      []byte
		ok     bool
		err    error
		sample core.Sample
	)
	sample, err = t.reader.Read()
	if err != nil {
		return nil, err
	}

	example := core.NewTFExample()
	for k, v := range sample {
		if b, ok = v.([]byte); !ok {
			b, err = jsoniter.Marshal(v)
			if err != nil {
				return nil, err
			}
		}
		example.AddBytes(k, b)
	}
	return example, nil
}

func (t *SampleToTFExamplesTypesTransformer) Read() (*core.TFExample, error) {
	var (
		ty     cmn.TFFeatureType
		sample core.Sample
		b      []byte
		ok     bool
		err    error
	)
	sample, err = t.reader.Read()
	if err != nil {
		return nil, err
	}

	example := core.NewTFExample()
	for k, v := range sample {
		if ty, ok = t.typesMap[k]; !ok {
			b, err := jsoniter.Marshal(v)
			if err != nil {
				return nil, err
			}
			example.AddBytes(k, b)
			continue
		}

		switch ty.FeatureType() {
		case cmn.Int64Type:
			var i int64
			if i, ok = v.(int64); !ok {
				i, err = binary.ReadVarint(bytes.NewBuffer(v.([]byte)))
				if err != nil {
					return nil, err
				}
			}
			example.AddInt64(k, i)
			continue
		case cmn.Int64ListType:
			var i []int64
			if i, ok = v.([]int64); !ok {
				err = binary.Read(bytes.NewBuffer(v.([]byte)), binary.LittleEndian, &i)
				if err != nil {
					return nil, err
				}
			}
			example.AddInt64List(k, i)
			continue
		case cmn.Float32Type:
			var i float32
			if i, ok = v.(float32); !ok {
				bits := binary.LittleEndian.Uint32(v.([]byte))
				i = math.Float32frombits(bits)
			}
			example.AddFloat(k, i)
			continue
		case cmn.Float32ListType:
			var i []float32
			if i, ok = v.([]float32); !ok {
				var ints []uint32
				err = binary.Read(bytes.NewBuffer(v.([]byte)), binary.LittleEndian, &ints)
				if err != nil {
					return nil, err
				}
				for _, bits := range ints {
					i = append(i, math.Float32frombits(bits))
				}
			}
			example.AddFloatList(k, i)
			continue
		case cmn.BytesType:
			example.AddBytes(k, v.([]byte))
			continue
		case cmn.BytesListType:
			example.AddBytesList(k, v.([][]byte))
			continue
		}

		if b, ok = v.([]byte); !ok {
			b, err = jsoniter.Marshal(v)
			if err != nil {
				return nil, err
			}
		}
		example.AddBytes(k, b)
	}
	return example, nil
}
