//// Package tassert provides test assertions
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package tassert

import (
	"runtime/debug"
	"testing"
)

func CheckFatal(t *testing.T, err error) {
	if err != nil {
		debug.PrintStack()
		t.Fatalf(err.Error())
	}
}

func CheckError(t *testing.T, err error) {
	if err != nil {
		debug.PrintStack()
		t.Errorf(err.Error())
	}
}


func Fatalf(t *testing.T, cond bool, msg string, args ...interface{}) {
	if !cond {
		debug.PrintStack()
		t.Fatalf(msg, args...)
	}
}

func Errorf(t *testing.T, cond bool, msg string, args ...interface{}) {
	if !cond {
		debug.PrintStack()
		t.Errorf(msg, args...)
	}
}
