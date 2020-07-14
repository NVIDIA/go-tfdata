// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

// Package cmn provides common low-level utilities for tfdata module
package cmn

func Assert(cond bool) {
	AssertMsg(cond, "assertion failed")
}

func AssertMsg(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

func AssertNoError(err error) {
	if err != nil {
		panic("assertion failed with error %v" + err.Error())
	}
}
