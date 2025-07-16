package main

import (
	"bytes"
	"encoding/binary"
)

// Represents kafka response message. Header contains a single field `correlation_id`.
// The Size field is a 32-bit signed integer. It specifies the size of the header and body.
// Both size and header are encoded in the BigEndian format.
type Response struct {
	Size    int32
	Header  int32
	Message any
}

func (res *Response) MarshalBinary() ([]byte, error) {
	b := new(bytes.Buffer)

	err := binary.Write(b, binary.BigEndian, res.Size)
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, res.Header)
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, res.Message)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}
