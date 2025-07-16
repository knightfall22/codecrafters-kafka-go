package main

import (
	"bufio"
	"fmt"
	"io"
	"sync/atomic"
)

type Processor struct {
	id int64
	rd *bufio.Reader
	wr io.Writer
}

var count atomic.Int64

func NewProcessor(input io.ReadWriter) *Processor {
	return &Processor{
		id: count.Add(1),
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

	fmt.Printf("[%d] Woe to you demon of the night: %v\n", p.id, byt)

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
