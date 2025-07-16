package main

import (
	"bufio"
	"io"
)

type Processor struct {
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

	byt, err = request.MarshalResponse()
	if err != nil {
		return err
	}

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
