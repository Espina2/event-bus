package main

import (
	"context"
	"github.com/Espina2/event-bus/kafka"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"log"
)

func main() {
	zapLog, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger := zapr.NewLogger(zapLog)

	ctx := context.Background()
	kafkaBus, err := kafka.NewKafkaEventBus(
		[]string{"localhost:29092", "localhost:39092"},
		"app-name",
		logger,
	)

	go func() {
		err = kafkaBus.Subscribe(ctx, "teste", func(payload []byte) error {
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	select {}
}
