package produce

import (
	"context"
	"errors"

	"github.com/syralon/syerrors"
)

type Middleware func(next Producer) Producer

func WithMiddlewares(p Producer, middlewares ...Middleware) Producer {
	for _, m := range middlewares {
		p = m(p)
	}
	return p
}

func Recovery() Middleware {
	return func(next Producer) Producer {
		return ProducerFunc(func(ctx context.Context, topic string, data ...[]byte) (err error) {
			defer func() {
				err = errors.Join(err, syerrors.Recovery(recover()))
			}()
			return next.Produce(ctx, topic, data...)
		})
	}
}
