// Package tfdata provides interfaces to interact with TFRecord files and TFExamples.
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//
package core

const KeyEntry = "__key__"

type (
	Sample struct {
		Entries map[string]interface{}
	}
)

func NewSample(entries ...map[string]interface{}) *Sample {
	if len(entries) > 0 {
		return &Sample{Entries: entries[0]}
	}

	return &Sample{Entries: make(map[string]interface{})}
}
