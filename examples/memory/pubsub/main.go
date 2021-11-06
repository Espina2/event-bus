package main

import (
	"context"
	"encoding/json"
	"github.com/Espina2/event-bus/memory"
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
	memoryBus := memory.NewInMemoryBus(logger)

	go func() {
		err := memoryBus.Subscribe(ctx, "teste", func(payload []byte) error {
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

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

					err = memoryBus.Publish(ctx, "teste", evtmarsh)
					if err != nil {
						log.Fatal(err)
					}
				}()
			}
		}
	}()

	time.Sleep(60 * time.Minute)
	ticker.Stop()
	done <- true
}
