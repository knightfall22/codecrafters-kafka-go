package main

import (
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
	// size := make([]byte, 4)
	byt, err := io.ReadAll(p.rd)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// msgLength := int32(binary.BigEndian.Uint32(size))

	// msg :=
	return byt, nil
}
