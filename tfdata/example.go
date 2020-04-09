//// Package tfdata provides interface to interact with TFRecord files and tf.Examples
///*
// * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
// */
//
package tfdata

import (
	"fmt"

	"github.com/NVIDIA/go-tfdata/proto"
	protobuf "google.golang.org/protobuf/proto"
)

type (
	TFExample struct {
		proto.Example
	}
)

func NewTFExample() *TFExample {
	ex := proto.Example{
		Features: &proto.Features{Feature: make(map[string]*proto.Feature)},
	}

	//ex.ProtoReflect().Descriptor()
	return &TFExample{ex}
}

func Marshal(e *TFExample) ([]byte, error) {
	return protobuf.Marshal(e)
}

func Unmarshal(p []byte, e *TFExample, schemaExample ...*TFExample) (err error) {
	if len(schemaExample) == 0 {
		return protobuf.Unmarshal(p, e)
	}

	schema := schemaExample[0]
	msg := schema.ProtoReflect().New().Interface()
	//descriptorProto := protodesc.ToDescriptorProto(schema.ProtoReflect().Descriptor())
	//err = protobuf.Unmarshal(p, descriptorProto)
	//
	//msg := dynamicpb.NewMessage(schema.ProtoReflect().Descriptor())
	err = protobuf.Unmarshal(p, msg)
	if err != nil {
		return err
	}

	if ex, ok := msg.(*TFExample); ok {
		*e = *ex
		return nil
	}
	return fmt.Errorf("couldn't parse message into TFExample")
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
