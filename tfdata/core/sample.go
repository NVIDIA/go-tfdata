// Package tfdata provides interfaces to interact with TFRecord files and TFExamples.
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package core

type (
	Sample struct {
		Name    string
		Entries map[string][]byte
	}
)

func NewSample(name string, entries ...map[string][]byte) *Sample {
	if len(entries) > 0 {
		return &Sample{Name: name, Entries: entries[0]}
	}

	return &Sample{Name: name, Entries: make(map[string][]byte)}
}
