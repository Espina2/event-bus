package kafka

import (
	"context"
	eventbus "github.com/Espina2/event-bus"
	"github.com/go-logr/logr"
	"github.com/segmentio/kafka-go"
	"sync"
	"time"
)

type EventBus struct {
	logger logr.Logger
	brokers []string
	consumerGroup string
	writerConn    *kafka.Writer
	readersConn map[string]*kafka.Reader
	lock     sync.Mutex
}

// NewKafkaEventBus creates a EventBus
func NewKafkaEventBus(brokers []string, consumerGroup string, logger logr.Logger) (*EventBus, error) {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}

	return &EventBus{
		logger: logger,
		brokers:       brokers,
		consumerGroup: consumerGroup,
		writerConn:    w,
		readersConn: make(map[string]*kafka.Reader),
		lock:          sync.Mutex{},
	}, nil
}

// Publish the event in the topic
func (bus *EventBus) Publish(ctx context.Context, eventName string, payload []byte) error {
	err := bus.writerConn.WriteMessages(ctx, kafka.Message{
		Topic: eventName,
		Value: payload,
	})
	if err != nil {
		bus.logger.Error(err, "Failed to published event", eventName, "payload", string(payload))
		return err
	}

	bus.logger.Info("Event Published", "eventName", eventName, "payload", string(payload))

	return nil
}

// Subscribe receives all the publish events in topic
func (bus *EventBus) Subscribe(ctx context.Context, eventName string, fn eventbus.FnHandler) error {
	r, ok := bus.readersConn[eventName]
	if !ok {
		r = kafka.NewReader(kafka.ReaderConfig{
			Brokers: bus.brokers,
			GroupID: bus.consumerGroup,
			Topic:   eventName,
		})

		bus.readersConn[eventName] = r
	}

	bus.logger.Info("Event Subscribed", "eventName", eventName)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}

		bus.logger.Info("Event Received", "eventName", eventName, "payload", string(m.Value))
		err = fn(m.Value)
		if err != nil {
			bus.logger.Error(err, "Failed to handle event with error", "eventName", eventName, "payload", string(m.Value))
		}

		_ = r.CommitMessages(ctx, m)
	}

	defer func(r *kafka.Reader) {
		_ = r.Close()
	}(r)

	return nil
}

// Close blocks until all Published events complete
func (bus *EventBus) Close() {
	err := bus.writerConn.Close()
	if err != nil {
		bus.logger.Error(err, "Failed to close kafka writer connection")
	}

	for i, _ := range bus.readersConn {
		err = bus.readersConn[i].Close()

		if err != nil {
			bus.logger.Error(err, "Failed to close kafka reader connection")
		}
	}
}
