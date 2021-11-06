package eventbus

import "context"

type EventBus interface {
	Publish(ctx context.Context, eventName string, payload []byte) error
	Subscribe(ctx context.Context, eventName string, fn FnHandler) error
	Close()
}

type FnHandler func(payload []byte) error
