package main

import (
	"bytes"
	"encoding/binary"
	"log"
)

// Represents kafka response message. Header contains a single field `correlation_id`.
// The Size field is a 32-bit signed integer. It specifies the size of the header and body.
// Both size and header are encoded in the BigEndian format.
type Response struct {
	size    int32
	header  int32
	Message any
}

func (res *Response) MarshalBinary() ([]byte, error) {
	b := new(bytes.Buffer)

	err := binary.Write(b, binary.BigEndian, res.size)
	if err != nil {
		return nil, err
	}

	err = binary.Write(b, binary.BigEndian, res.header)
	if err != nil {
		return nil, err
	}

	// err = binary.Write(b, binary.BigEndian, res.Message)
	// if err != nil {
	// 	return nil, err
	// }

	return b.Bytes(), nil
}

type RequestHeader struct {
	//The API key for the request
	request_api_key int16

	//The version of the API for the request
	request_api_version int16

	//A unique identifier for the request
	correlationId int32

	//...igore these fields for now
	//The client ID for the request
	client_id any

	//Optional tagged fields
	tagged_fields any
}

// Represenst a v2 Kafka response message
type Request struct {
	size    int32
	header  RequestHeader
	Message any
}

func (req *Request) UnmarshallBinary(p []byte) error {
	log.Printf("Reading \n")
	r := bytes.NewReader(p)

	err := binary.Read(r, binary.BigEndian, &req.size)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.BigEndian, &req.header.request_api_key)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.BigEndian, &req.header.request_api_version)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.BigEndian, &req.header.correlationId)
	if err != nil {
		return err
	}

	return nil
}

func (req *Request) MarshalResponse() ([]byte, error) {
	resBody := Response{
		size:   req.size,
		header: req.header.correlationId,
	}

	return resBody.MarshalBinary()
}
