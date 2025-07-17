package kafka

import (
	"bytes"
	"encoding/binary"
	"io"
	"slices"

	"github.com/codecrafters-io/kafka-starter-go/app/commons"
)

// Representation of Kafka api protocols
type ApiProtocols int16

const (
	Produce                 ApiProtocols = 0
	Fetch                   ApiProtocols = 1
	DescribeTopicPartitions ApiProtocols = 75
	ApiVersions             ApiProtocols = 18
)

// Map api protocols to versions
var ApiProtocolsVersions = map[ApiProtocols][]int16{
	ApiVersions:             {0, 1, 2, 3, 4},
	DescribeTopicPartitions: {0},
}

// Validate version of `APIProtocol`. Also validate if protocol exists
// - when attempting to validate the protocol make v == 0
func (pr ApiProtocols) ValidateVersion(v int16) ApiErrorCodes {
	versions, ok := ApiProtocolsVersions[pr]
	if !ok {
		return UNSUPPORTED_VERSION
	}

	if exists := slices.Contains(versions, v); !exists {
		return UNSUPPORTED_VERSION
	}

	return NONE
}

func (pr ApiProtocols) getVersions() []int16 {
	versions, ok := ApiProtocolsVersions[pr]
	if !ok {
		return nil
	}

	return versions
}

type ApiErrorCodes int16

const (
	NONE                       = 0
	UNSUPPORTED_VERSION        = 35
	UNKNOWN_TOPIC_OR_PARTITION = 3
)

type CompactString struct {
	length  commons.UVarint
	content []byte
}

func ReadCompactString(buf *bytes.Reader) (CompactString, error) {
	length, err := commons.ReadUVarint(buf)
	if err != nil {
		return CompactString{}, err
	}

	contents := make([]byte, length-1)
	_, err = io.ReadFull(buf, contents)
	if err != nil {
		return CompactString{}, err
	}

	return CompactString{
		length:  length,
		content: contents,
	}, nil
}

func WriteCompactString(buf *bytes.Buffer, c CompactString) error {
	err := commons.WriteUvarint(buf, c.length)
	if err != nil {
		return err
	}

	return binary.Write(buf, binary.BigEndian, c.content)
}
