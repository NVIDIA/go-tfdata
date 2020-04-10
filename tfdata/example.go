// Package tfdata provides interfaces to interact with TFRecord files and TFExamples.
//
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package tfdata

import (
	"github.com/NVIDIA/go-tfdata/proto"
)

type (
	TFExamplePipe chan *TFExample

	// TFExample is a wrapper over proto.Example struct generated by protoc from TensorFlow
	// tf.Example proto files. It is a golang representation of tf.Example datastructure.
	// It includes functions for adding elements to tf.Example.Features.
	TFExample struct {
		proto.Example
	}
)

// NewTFExample initializes empty TFExample and returns it.
func NewTFExample() *TFExample {
	ex := proto.Example{
		Features: &proto.Features{Feature: make(map[string]*proto.Feature)},
	}

	return &TFExample{ex}
}

func (e *TFExample) AddInt64List(name string, ints []int64) {
	e.Features.Feature[name] = &proto.Feature{Kind: &proto.Feature_Int64List{Int64List: &proto.Int64List{Value: ints}}}
}

func (e *TFExample) AddIntList(name string, ints []int) {
	ints64 := make([]int64, 0, len(ints))
	for _, i := range ints {
		ints64 = append(ints64, int64(i))
	}
	e.AddInt64List(name, ints64)
}

func (e *TFExample) AddInt64(name string, ints ...int64) {
	e.AddInt64List(name, ints)
}

func (e *TFExample) AddInt(name string, ints ...int) {
	e.AddIntList(name, ints)
}

func (e *TFExample) AddFloatList(name string, floats []float32) {
	e.Features.Feature[name] = &proto.Feature{Kind: &proto.Feature_FloatList{FloatList: &proto.FloatList{Value: floats}}}
}

func (e *TFExample) AddFloat(name string, floats ...float32) {
	e.AddFloatList(name, floats)
}

func (e *TFExample) AddBytesList(name string, bytes [][]byte) {
	e.Features.Feature[name] = &proto.Feature{Kind: &proto.Feature_BytesList{BytesList: &proto.BytesList{Value: bytes}}}
}

func (e *TFExample) AddBytes(name string, bytes ...[]byte) {
	e.AddBytesList(name, bytes)
}
