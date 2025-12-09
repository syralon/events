package produce

import "context"

type Producer interface {
	Produce(ctx context.Context, topic string, data ...[]byte) error
}

type ProducerFunc func(ctx context.Context, topic string, data ...[]byte) error

func (fn ProducerFunc) Produce(ctx context.Context, topic string, data ...[]byte) error {
	return fn(ctx, topic, data...)
}
