package consume

import "context"

type Consumer interface {
	Consume(ctx context.Context, topic string, handler Handler) error
}
