package redisbus

import (
	"encoding/json"
)

type MessageEncoder[M any] interface {
	EncodeMessage(message M) ([]byte, error)
	DecodeMessage(data []byte, message *M) error
}

type JSONMessageEncoder[M any] struct{}

func (e JSONMessageEncoder[M]) EncodeMessage(message M) ([]byte, error) {
	return json.Marshal(message)
}

func (e JSONMessageEncoder[M]) DecodeMessage(data []byte, message *M) error {
	return json.Unmarshal(data, message)
}
