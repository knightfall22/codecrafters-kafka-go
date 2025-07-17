package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

type ClientID struct {
	length   int16
	contents []byte
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
	client_id ClientID

	//Optional tagged fields
	tagged_fields TaggedFields
}

// Represenst a v2 Kafka response message
type Request struct {
	header RequestHeader
	body   *bytes.Reader
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

	err = binary.Read(r, binary.BigEndian, &req.header.client_id.length)
	if err != nil {
		return nil, err
	}

	contents := make([]byte, req.header.client_id.length)
	fmt.Println(req)
	_, err = io.ReadFull(r, contents)
	if err != nil {
		return nil, err
	}
	req.header.client_id.contents = contents

	err = binary.Read(r, binary.BigEndian, &req.header.tagged_fields)
	if err != nil {
		return nil, err
	}

	req.body = r

	return &req, nil
}

func (req *Request) HandleRequest() ([]byte, error) {
	fmt.Println(req)

	switch req.header.request_api_key {
	case ApiVersions:
		request := APIVersionRequest{RequestHeader: req.header}
		return request.MarshalBinary()
	case DescribeTopicPartitions:
		request := DescribeTopicPartitionsRequest{RequestHeader: req.header, RequestBodyRaw: req.body}
		return request.MarshalBinary()
	default:
		//todo: change to proper unsupported request error message
		err := fmt.Errorf("unsupported request: %d", req.header.request_api_key)
		return nil, err
	}
}
