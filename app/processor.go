package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
)

type Processor struct {
	id atomic.Int64
	rd io.Reader
	wr io.Writer
}

func NewProcessor(input io.ReadWriter) *Processor {
	return &Processor{
		rd: input,
		wr: input,
	}
}

func (p *Processor) Process() error {
	byt, err := p.read()

	var request Request
	err = request.UnmarshallBinary(byt)
	if err != nil {
		return err
	}

	byt, err = request.HandleRequest()
	if err != nil {
		return err
	}

	fmt.Printf("Woe to you demon of the night: %v\n", byt)

	_, err = p.wr.Write(byt)
	if err != nil {
		return err
	}

	return nil
}

func (p *Processor) read() ([]byte, error) {
	//read the 4 byte message size
	size := make([]byte, 4)
	_, err := io.ReadFull(p.rd, size)
	if err != nil && err != io.EOF {
		return nil, err
	}

	msgLength := int32(binary.BigEndian.Uint32(size))

	//Read the request body
	msg := make([]byte, msgLength)
	_, err = io.ReadFull(p.rd, msg)
	if err != nil && err != io.EOF {
		return nil, err
	}

	//combine size and body
	req := append(size, msg...)
	return req, nil
}
