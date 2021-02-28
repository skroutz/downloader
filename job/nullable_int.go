package job

import (
	"bytes"
	"encoding/json"
)

// NullableInt represents an int that can be "empty" (aka nil).
//
// A NullableInt is empty by default.
type NullableInt struct {
	hasValue bool // false by default
	i        int
}

// IsNil returns true if the value is empty.
func (ni *NullableInt) IsNil() bool {
	return !ni.hasValue
}

// Int returns the value, make sure to check ISNil before using Int.
func (ni *NullableInt) Value() int {
	return ni.i
}

// Set sets the value to i.
func (ni *NullableInt) Set(i int) {
	ni.hasValue = true
	ni.i = i
}

// MarshalBinary encodes used a NullableInt. Implements encoding.BinaryMarshaler.
// ni is encoded to JSON. null if empty, int otherwise.
func (ni NullableInt) MarshalBinary() (data []byte, err error) {
	if ni.IsNil() {
		return json.Marshal(nil)
	}

	return json.Marshal(ni.i)
}

// UnmarshalBinary decodes data to a NullableInt. Implements encoding.BinaryUnmarshaler.
func (ni *NullableInt) UnmarshalBinary(data []byte) error {
	if bytes.Equal(data, []byte("null")) { // json.Marshal(nil)
		ni.hasValue = false
		return nil
	}

	ni.hasValue = true
	return json.Unmarshal(data, &ni.i)
}
