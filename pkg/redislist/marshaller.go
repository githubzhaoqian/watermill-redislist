package redislist

import (
	"encoding/json"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

const UUIDHeaderKey = "_watermill_message_uuid"

// Marshaler marshals Watermill's message to Kafka message.
type Marshaller interface {
	Marshal(topic string, msg *message.Message) (string, error)
}

// Unmarshaler unmarshals Kafka's message to Watermill's message.
type Unmarshaller interface {
	Unmarshal(value string) (*message.Message, error)
}

type MarshallerUnmarshaller interface {
	Marshaller
	Unmarshaller
}

type DefaultMarshallerUnmarshaller struct{}

func (DefaultMarshallerUnmarshaller) Marshal(topic string, msg *message.Message) (string, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return "", errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}
	member := &Member{
		Topic:    topic,
		UUID:     msg.UUID,
		Payload:  msg.Payload,
		Metadata: msg.Metadata,
	}
	memberBytes, err := json.Marshal(member)
	if err != nil {
		return "", errors.Wrapf(err, "member json.Marshal")
	}
	return string(memberBytes), nil
}

func (DefaultMarshallerUnmarshaller) Unmarshal(value string) (*message.Message, error) {
	var messageID string
	member := &Member{}
	err := json.Unmarshal([]byte(value), member)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	metadata := member.Metadata
	msg := message.NewMessage(messageID, member.Payload)
	msg.Metadata = metadata
	msg.UUID = member.UUID
	return msg, nil
}

func getKey(topic, id string) string {
	return fmt.Sprintf("%s:%s", topic, id)
}

func getLockKey(topic, id string) string {
	return fmt.Sprintf("%s:lock:%s", topic, id)
}
