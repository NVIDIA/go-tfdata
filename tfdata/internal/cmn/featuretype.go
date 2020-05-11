// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

package cmn

type (
	// internal, intentionally users can't implement it
	TFFeatureType interface {
		FeatureType() int
	}

	TFFeatureInt       struct{}
	TFFeatureIntList   struct{}
	TFFeatureFloat     struct{}
	TFFeatureFloatList struct{}
	TFFeatureBytes     struct{}
	TFFeatureBytesList struct{}
)

const (
	Int64Type = iota
	Int64ListType
	Float32Type
	Float32ListType
	BytesType
	BytesListType
)

func (*TFFeatureInt) FeatureType() int       { return Int64Type }
func (*TFFeatureIntList) FeatureType() int   { return Int64ListType }
func (*TFFeatureFloat) FeatureType() int     { return Float32Type }
func (*TFFeatureFloatList) FeatureType() int { return Float32ListType }
func (*TFFeatureBytes) FeatureType() int     { return BytesType }
func (*TFFeatureBytesList) FeatureType() int { return BytesListType }
