package memory

import (
	"context"
	eventbus "github.com/Espina2/event-bus"
	"github.com/go-logr/logr"
	"sync"
)

type InMemoryEventBus struct {
	handlers map[string][]eventbus.FnHandler
	lock     sync.Mutex
	wg sync.WaitGroup
	logger logr.Logger
}

// NewInMemoryBus creates an InMemoryEventBus
func NewInMemoryBus(logger logr.Logger) *InMemoryEventBus {
	return &InMemoryEventBus{
		handlers: make(map[string][]eventbus.FnHandler),
		wg: sync.WaitGroup{},
		lock:     sync.Mutex{},
		logger: logger,
	}
}

// Publish sends the Event for All the subscribers
func (bus *InMemoryEventBus) Publish(ctx context.Context, eventName string, payload []byte) error {
	bus.wg.Add(1)
	bus.lock.Lock()
	defer bus.lock.Unlock()

	if handlers, ok := bus.handlers[eventName]; ok && 0 < len(handlers) {
		for i, _ := range handlers {
			index := i

			go func() {
				defer bus.wg.Done()
				bus.logger.Info("Event Published", "eventName", eventName, "payload", string(payload))
				err := handlers[index](payload)
				if err != nil {
					bus.logger.Error(err, "Failed to handle event with error", eventName, "payload", string(payload))
				}

				bus.logger.Info("Event Handled", "eventName", eventName, "payload", string(payload))
			}()
		}
	}

	return nil
}

// Subscribe receives all the publish events
func (bus *InMemoryEventBus) Subscribe(ctx context.Context, eventName string, fn eventbus.FnHandler) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	bus.handlers[eventName] = append(bus.handlers[eventName], fn)
	bus.logger.Info("Event Subscribed", "eventName", eventName)
	return nil
}

// Close blocks until all Published events complete
func (bus *InMemoryEventBus) Close() {
	bus.wg.Wait()
}

