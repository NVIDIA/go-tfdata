//// Package internal provides internal, not available in public API functions and structures used by tfdata package
//
// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
//

package checksum

import (
	"fmt"
	"hash/crc32"
)

const tfRecordCRCMask = 0xa282ead8

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

type (
	Checksummer interface {
		Get(p []byte) uint32
		Verify(p []byte, cksm uint32) error
		Type() string
	}

	CrcChecksummer struct{}

	InvalidChecksumError struct {
	}
)

func (CrcChecksummer) Get(p []byte) uint32 {
	crc := crc32.Checksum(p, crc32Table)
	return ((crc >> 15) | (crc << 17)) + tfRecordCRCMask
}

func (c CrcChecksummer) Verify(p []byte, expected uint32) error {
	actual := c.Get(p)
	if c.Get(p) != expected {
		return fmt.Errorf("invalid checksum %s; got %d, expected %d", c.Type(), actual, expected)
	}
	return nil
}

func (CrcChecksummer) Type() string {
	return fmt.Sprintf("crc (poly:Castagnoli;mask%x)", tfRecordCRCMask)
}

func NewCRCChecksummer() CrcChecksummer { return CrcChecksummer{} }
