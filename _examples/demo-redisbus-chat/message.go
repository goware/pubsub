package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type Message interface {
	Type() string
}

type ConnectMessage struct {
	From string `json:"from"`
}

func (m ConnectMessage) Type() string {
	return "connect"
}

type ChatMessage struct {
	From string `json:"from"`
	Text string `json:"text"`
}

func (m ChatMessage) Type() string {
	return "chat"
}

func init() {
	gob.Register(ConnectMessage{})
	gob.Register(ChatMessage{})
}

type MessageEncoder[M any] struct{}

func (e MessageEncoder[M]) EncodeMessage(message M) ([]byte, error) {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := enc.Encode(&message); err != nil {
		return nil, fmt.Errorf("failed to encode message: %w", err)
	}
	return data.Bytes(), nil
}

func (e MessageEncoder[M]) DecodeMessage(data []byte, message *M) error {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err := dec.Decode(message)
	if err != nil {
		return fmt.Errorf("failed to decode message: %w", err)
	}
	return nil
}
