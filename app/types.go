package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"slices"
)

// Represents kafka response message. Header contains a single field `correlation_id`.
// The Size field is a 32-bit signed integer. It specifies the size of the header and body.
// Both size and header are encoded in the BigEndian format.
type Response struct {
	size     int32
	header   int32
	Message  any
	errorMsg ApiErrorCodes
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

	err = binary.Write(b, binary.BigEndian, res.errorMsg)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

type RequestHeader struct {
	//The API key for the request
	request_api_key ApiProtocols

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
	//.....
	//could be change later
	errorMsg ApiErrorCodes
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

	req.validateVersion()

	return nil
}

func (req *Request) validateVersion() {
	code := req.header.request_api_key.ValidateVersion(req.header.request_api_version)
	req.errorMsg = code
}

func (req *Request) MarshalResponse() ([]byte, error) {
	resBody := Response{
		size:     req.size,
		header:   req.header.correlationId,
		errorMsg: req.errorMsg,
	}

	return resBody.MarshalBinary()
}

// Representation of Kafka api protocols
type ApiProtocols int16

const (
	Produce     ApiProtocols = 0
	Fetch       ApiProtocols = 1
	ApiVersions ApiProtocols = 18
)

// Map api protocols to versions
var ApiProtocolsVersions = map[ApiProtocols][]int16{
	ApiVersions: {0, 1, 2, 3, 4},
}

// Validate version of `APIProtocol`. Also validate if protocol exists
// -when attempting to validate the protocol make v == 0
func (pr ApiProtocols) ValidateVersion(v int16) ApiErrorCodes {
	versions, ok := ApiProtocolsVersions[pr]
	if !ok {
		return unsupported_version
	}

	if exists := slices.Contains(versions, v); !exists {
		return unsupported_version
	}

	return none
}

type ApiErrorCodes int16

const (
	none                = 0
	unsupported_version = 35
)
