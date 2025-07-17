package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/commons"
)

type DescribeTopicPartitionsRequest struct {
	RequestHeader
	DTPRequestBody
	RequestBodyRaw *bytes.Reader
}

func (req *DescribeTopicPartitionsRequest) MarshalBinary() ([]byte, error) {
	err := req.parse()
	if err != nil {
		return nil, err
	}

	res := req.generateResponse()
	fmt.Printf("res: %+v\n", res)
	return res.marshall()
}

func (req *DescribeTopicPartitionsRequest) parse() error {
	arrLen, err := commons.ReadUVarint(req.RequestBodyRaw)
	if err != nil {
		return err
	}

	req.topicArrayLength = arrLen

	for range arrLen - 1 {
		var topic topic
		compact, err := ReadCompactString(req.RequestBodyRaw)
		if err != nil {
			return err
		}

		err = binary.Read(req.RequestBodyRaw, binary.BigEndian, &topic.topicTagBuffer)
		if err != nil {
			return err
		}
		topic.topicName = compact
		req.topic = append(req.topic,
			topic,
		)
	}

	err = binary.Read(req.RequestBodyRaw, binary.BigEndian, &req.responsePartitionLimit)
	if err != nil {
		return err
	}

	err = binary.Read(req.RequestBodyRaw, binary.BigEndian, &req.cursor)
	if err != nil {
		return err
	}

	err = binary.Read(req.RequestBodyRaw, binary.BigEndian, &req.taggedBuffer)
	if err != nil {
		return err
	}

	return nil
}

func (req *DescribeTopicPartitionsRequest) generateResponse() *DescribeTopicPartitionsResponse {
	var res DescribeTopicPartitionsResponseBody

	res.topicsArrayLength = req.topicArrayLength

	tArrLen := len(req.topic) - 1

	res.topic = make([]responseTopic, tArrLen)

	for i := range tArrLen {
		res.topic[i].topicName.length = req.topic[i].topicName.length
		res.topic[i].topicName.content = req.topic[i].topicName.content

		res.topic[i].err = UNKNOWN_TOPIC_OR_PARTITION
		res.topic[i].partitionsArrayLength = 1
	}

	res.nextCursor = 0xFF

	return &DescribeTopicPartitionsResponse{
		ResponseHeader{
			CorrelationID: req.correlationId,
		},
		res,
	}
}

type topic struct {
	topicName      CompactString //compact string
	topicTagBuffer TaggedFields
}

type DTPRequestBody struct {
	topicArrayLength       commons.UVarint //Compact array
	topic                  []topic
	responsePartitionLimit int32
	cursor                 int8
	taggedBuffer           TaggedFields
}

type DescribeTopicPartitionsResponse struct {
	ResponseHeader
	DescribeTopicPartitionsResponseBody
}

type partitions struct{}

type responseTopic struct {
	err                   ApiErrorCodes
	topicName             CompactString
	topicID               [16]byte
	isInternal            int8
	partitionsArrayLength commons.UVarint
	partitions            []partitions
	taggedBuffer          TaggedFields
}

type DescribeTopicPartitionsResponseBody struct {
	throttleTimeMs    int32
	topicsArrayLength commons.UVarint
	topic             []responseTopic
	nextCursor        byte
	taggedBuffer      TaggedFields
}

func (res *DescribeTopicPartitionsResponse) marshall() ([]byte, error) {
	p := &bytes.Buffer{}

	err := binary.Write(p, binary.BigEndian, res.CorrelationID)
	if err != nil {
		return nil, err
	}

	err = binary.Write(p, binary.BigEndian, res.taggedBuffer)
	if err != nil {
		return nil, err
	}

	err = binary.Write(p, binary.BigEndian, res.throttleTimeMs)
	if err != nil {
		return nil, err
	}

	err = commons.WriteUvarint(p, res.topicsArrayLength)
	if err != nil {
		return nil, err
	}

	for i := range len(res.topic) {
		err = binary.Write(p, binary.BigEndian, res.topic[i].err)
		if err != nil {
			return nil, err
		}

		err = WriteCompactString(p, res.topic[i].topicName)
		if err != nil {
			return nil, err
		}

		err = binary.Write(p, binary.BigEndian, res.topic[i].topicID)
		if err != nil {
			return nil, err
		}

		err = binary.Write(p, binary.BigEndian, res.topic[i].isInternal)
		if err != nil {
			return nil, err
		}

		err = binary.Write(p, binary.BigEndian, res.topic[i].isInternal)
		if err != nil {
			return nil, err
		}

		err = commons.WriteUvarint(p, res.topic[i].partitionsArrayLength)
		if err != nil {
			return nil, err
		}

		// for _, p := range res.topic[i].partitions {

		// }
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
