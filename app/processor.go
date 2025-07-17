package main

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka"
)

type Processor struct {
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

	request, err := kafka.UnmarshallRequest(byt)
	if err != nil {
		return err
	}

	byt, err = request.HandleRequest()
	if err != nil {
		return err
	}

	fmt.Printf("This is : %+v\n", byt)

	_, err = p.wr.Write(byt)
	if err != nil {
		return err
	}

	return nil
}

// Note: the issue preventing serial message processing was cause
// by reading the entirity of the reader till io.EOF is returned
func (p *Processor) read() ([]byte, error) {
	//read the 4 byte message size
	size := make([]byte, 4)
	_, err := io.ReadFull(p.rd, size)
	if err != nil && err != io.EOF {
		return nil, err
	}

	msgLength := int32(binary.BigEndian.Uint32(size))

	//Read the request body
	req := make([]byte, msgLength)
	_, err = io.ReadFull(p.rd, req)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return req, nil
}
