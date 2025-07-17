package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

// Todo: if you can't find a use for this remove it
type ResponseBody interface {
	MarshalBinary() ([]byte, error)
}

type ResponseHeader struct {
	CorrelationID int32
	TaggedFields  TaggedFields
}

// Represents kafka response message. Header contains a single field `correlation_id`.
// The Size field is a 32-bit signed integer. It specifies the size of the header and body.
// Both size and header are encoded in the BigEndian format.
type Response interface {
	ResponseHeader
	ResponseBody
}

type TaggedFields int8

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
	tagged_fields TaggedFields
}

// Represenst a v2 Kafka response message
type Request struct {
	header RequestHeader
}

func UnmarshallRequest(p []byte) (*Request, error) {
	log.Printf("Reading \n")
	r := bytes.NewReader(p)

	var req Request

	err := binary.Read(r, binary.BigEndian, &req.header.request_api_key)
	if err != nil {
		return nil, err
	}

	err = binary.Read(r, binary.BigEndian, &req.header.request_api_version)
	if err != nil {
		return nil, err
	}

	err = binary.Read(r, binary.BigEndian, &req.header.correlationId)
	if err != nil {
		return nil, err
	}

	return &req, nil
}

func (req *Request) HandleRequest() ([]byte, error) {

	switch req.header.request_api_key {
	case ApiVersions:
		request := APIVersionRequest{RequestHeader: req.header}
		return request.MarshalBinary()
	default:
		//todo: change to proper unsupported request error message
		err := fmt.Errorf("unsupported request: %d", req.header.request_api_key)
		return nil, err
	}
}
