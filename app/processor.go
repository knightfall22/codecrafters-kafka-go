package main

import (
	"bufio"
	"fmt"
	"io"
	"sync/atomic"
)

type Processor struct {
	id atomic.Int64
	rd *bufio.Reader
	wr io.Writer
}

func NewProcessor(input io.ReadWriter) *Processor {
	return &Processor{
		rd: bufio.NewReader(input),
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
	byt, err := p.rd.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, err
	}

	return byt, nil
}
