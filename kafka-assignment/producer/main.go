package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func main() {

	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "example-123",
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		Async:                  true,
		Completion: func(messages []kafka.Message, err error) {

			if err != nil {
				fmt.Println(err)
				return
			}

			for _, val := range messages {
				fmt.Printf("messages sent, offset %d, key %s, val %s \n", val.Offset, val.Key, val.Value)
			}
		},
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("k 1"),
			Value: []byte("v 1"),
		},
		kafka.Message{
			Key:   []byte("k 2"),
			Value: []byte("v 2"),
		},
		kafka.Message{
			Key:   []byte("k 3"),
			Value: []byte("v 3"),
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
