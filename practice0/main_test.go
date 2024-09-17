package main

import (
	"bytes"
	"testing"
)

func TestFiller(t *testing.T) {
	b := make([]byte, 100)
	zero := byte('0')
	one := byte('1')
	filler(b, zero, one)

	if !bytes.Contains(b, []byte{zero}) {
		t.Errorf("Slice does not contain byte %v", zero)
	}

	if !bytes.Contains(b, []byte{one}) {
		t.Errorf("Slice does not contain byte %v", one)
	}
}
