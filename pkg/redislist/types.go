package redislist

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

type Member struct {
	Topic string `json:"topic"`
	UUID  string `json:"uuid"`

	// Metadata contains the message metadata.
	//
	// Can be used to store data which doesn't require unmarshalling the entire payload.
	// It is something similar to HTTP request's headers.
	//
	// Metadata is marshaled and will be saved to the PubSub.
	Metadata message.Metadata `json:"metadata"`

	// Payload is the message's payload.
	Payload message.Payload `json:"payload"`
}
