package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type Message struct {
	Body string `json:"body"`
}

func (m Message) Type() string {
	return "message"
}

type Event interface {
	Type() string
}

func init() {
	gob.Register(Message{})
	// add other types here..
}

type GobMessageEncoder[M any] struct {
}

func (e GobMessageEncoder[M]) EncodeMessage(message M) ([]byte, error) {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := enc.Encode(&message); err != nil {
		return nil, fmt.Errorf("failed to encode message: %w", err)
	}
	return data.Bytes(), nil
}

func (e GobMessageEncoder[M]) DecodeMessage(data []byte, message *M) error {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err := dec.Decode(message)
	if err != nil {
		return fmt.Errorf("failed to decode message: %w", err)
	}
	return nil
}
