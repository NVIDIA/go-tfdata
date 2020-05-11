// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

package core

// KeyEntry is a special-meaning entry in Sample and TFExample.
// Its value indicates basename of Sample in a source archive file.
const KeyEntry = "__key__"

type (
	Sample map[string]interface{}
)

func NewSample() Sample {
	return make(map[string]interface{})
}
