// Package cmn provides common low-level utilities for tfdata module
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

func Assert(cond bool) {
	if !cond {
		panic("assertion failed")
	}
}

func AssertNoError(err error) {
	if err != nil {
		panic("assertion failed with error %v" + err.Error())
	}
}
