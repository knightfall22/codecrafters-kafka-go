package kafka

import "slices"

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
		return unsupported_version
	}

	if exists := slices.Contains(versions, v); !exists {
		return unsupported_version
	}

	return none
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
	none                = 0
	unsupported_version = 35
)
