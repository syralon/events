package consume

import "context"

type Handler interface {
	Handle(ctx context.Context, topic string, data []byte) error
}

type HandlerFunc func(ctx context.Context, topic string, data []byte) error

func (fn HandlerFunc) Handle(ctx context.Context, topic string, data []byte) error {
	return fn(ctx, topic, data)
}
