package commons

import (
	"bytes"
	"encoding/binary"
)

type Varint int32

type UVarint int64

func ReadUVarint(b *bytes.Reader) (UVarint, error) {
	uv, err := binary.ReadUvarint(b)

	return UVarint(uv), err
}

func WriteUvarint(w *bytes.Buffer, x UVarint) error {
	var buf [10]byte
	i := 0
	for x >= 0x80 {
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	_, err := w.Write(buf[:i+1])
	return err
}
