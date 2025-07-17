package kafka

import (
	"bytes"
	"encoding/binary"
)

type APIVersionRequest struct {
	RequestHeader
}

func (req *APIVersionRequest) MarshalBinary() ([]byte, error) {
	res := req.generateResponse()
	return res.marshall()
}

func (req *APIVersionRequest) generateResponse() *ApiVersionsResponse {
	var res ApiVersionsResponseBody

	if req.request_api_version > 4 {
		res.err = unsupported_version
	} else {
		res.err = none
	}

	res.numberOfApis = int8(len(ApiProtocolsVersions)) + 1

	for k, v := range ApiProtocolsVersions {
		res.apiKeys = append(res.apiKeys,
			apiKeys{
				apiKey:       k,
				minSupported: v[0],
				maxSupported: v[len(v)-1],
			},
		)
	}

	return &ApiVersionsResponse{
		ResponseHeader{
			CorrelationID: req.correlationId,
		},
		res,
	}
}

type ApiVersionsResponse struct {
	ResponseHeader
	ApiVersionsResponseBody
}

type ApiVersionsResponseBody struct {
	err            ApiErrorCodes
	numberOfApis   int8
	apiKeys        []apiKeys
	throttleTimeMs int32
	taggedFields   TaggedFields
}

type apiKeys struct {
	apiKey       ApiProtocols
	minSupported int16
	maxSupported int16
	taggedFields TaggedFields
}

func (res *ApiVersionsResponse) marshall() ([]byte, error) {
	p := &bytes.Buffer{}

	err := binary.Write(p, binary.BigEndian, res.CorrelationID)
	if err != nil {
		return nil, err
	}

	err = binary.Write(p, binary.BigEndian, res.err)
	if err != nil {
		return nil, err
	}

	err = binary.Write(p, binary.BigEndian, res.numberOfApis)
	if err != nil {
		return nil, err
	}

	for i := range len(res.apiKeys) {
		err = binary.Write(p, binary.BigEndian, res.apiKeys[i].apiKey)
		if err != nil {
			return nil, err
		}

		err = binary.Write(p, binary.BigEndian, res.apiKeys[i].minSupported)
		if err != nil {
			return nil, err
		}

		err = binary.Write(p, binary.BigEndian, res.apiKeys[i].maxSupported)
		if err != nil {
			return nil, err
		}

		err = binary.Write(p, binary.BigEndian, res.apiKeys[i].taggedFields)
		if err != nil {
			return nil, err
		}
	}

	err = binary.Write(p, binary.BigEndian, res.throttleTimeMs)
	if err != nil {
		return nil, err
	}

	err = binary.Write(p, binary.BigEndian, res.taggedFields)
	if err != nil {
		return nil, err
	}

	payload := p.Bytes()
	size := int32(len(payload))

	buf := &bytes.Buffer{}

	err = binary.Write(buf, binary.BigEndian, size)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, payload)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
