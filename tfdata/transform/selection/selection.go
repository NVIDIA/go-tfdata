//// Package selection provides implementation of tfdata.Transformation
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package selection

import (
	"reflect"
	"strings"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
)

type (
	Sample interface {
		SelectSample(*core.Sample) []string
	}

	Example interface {
		SelectExample(*core.TFExample) []string
	}

	Key struct {
		key, suffix, prefix, substring string
	}

	SampleF struct {
		f func(*core.Sample) []string
	}

	ExampleF struct {
		f func(*core.TFExample) []string
	}

	KeyValue struct {
		key   string
		value interface{}
	}
)

var (
	_, _, _ Sample  = &Key{}, &SampleF{}, &KeyValue{}
	_, _, _ Example = &Key{}, &ExampleF{}, &KeyValue{}
)

func ByKey(key string) *Key {
	return &Key{key: key}
}

func ByPrefix(prefix string) *Key {
	return &Key{prefix: prefix}
}

func BySuffix(sufix string) *Key {
	return &Key{suffix: sufix}
}

func BySubstring(substring string) *Key {
	return &Key{substring: substring}
}

func (s *Key) SelectSample(sample *core.Sample) []string {
	res := make([]string, 0)
	for k := range sample.Entries {
		if k == s.key || strings.HasPrefix(k, s.prefix) || strings.HasSuffix(k, s.suffix) || strings.Contains(k, s.substring) {
			res = append(res, k)
		}
	}
	return res
}

func (s *Key) SelectExample(ex *core.TFExample) []string {
	res := make([]string, 0)
	for k := range ex.GetFeatures().Feature {
		if k == s.key || strings.HasPrefix(k, s.prefix) || strings.HasSuffix(k, s.suffix) || strings.Contains(k, s.substring) {
			res = append(res, k)
		}
	}
	return res
}

func BySampleF(f func(*core.Sample) []string) *SampleF {
	return &SampleF{f: f}
}

func (s *SampleF) SelectSample(sample *core.Sample) []string {
	return s.f(sample)
}

func ByExampleF(f func(*core.TFExample) []string) *ExampleF {
	return &ExampleF{f: f}
}

func (s *ExampleF) SelectExample(ex *core.TFExample) []string {
	return s.f(ex)
}

func ByKeyValue(key string, value interface{}) *KeyValue {
	return &KeyValue{key: key, value: value}
}

func (s *KeyValue) SelectSample(sample *core.Sample) []string {
	res := make([]string, 0)
	for k, v := range sample.Entries {
		if reflect.DeepEqual(v, s.value) {
			res = append(res, k)
		}
	}
	return res
}

func (s *KeyValue) SelectExample(example *core.TFExample) []string {
	res := make([]string, 0)
	for k, v := range example.GetFeatures().Feature {
		if reflect.DeepEqual(v, s.value) {
			res = append(res, k)
		}
	}
	return res
}
