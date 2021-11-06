package main

import (
	"context"
	"encoding/json"
	"github.com/Espina2/event-bus/kafka"
	"github.com/go-logr/zapr"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"log"
	"time"
)

type Event struct {
	Id        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

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
	if err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				go func() {
					evt := Event{
						Id:        uuid.New().String(),
						CreatedAt: time.Now(),
					}
					evtmarsh, _ := json.Marshal(evt)

					err = kafkaBus.Publish(ctx, "teste", evtmarsh)
					if err != nil {
						log.Fatal(err)
					}
				}()
			}
		}
	}()

	time.Sleep(1 * time.Minute)
	ticker.Stop()
	done <- true
	kafkaBus.Close()
}
