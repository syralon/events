package consume

import (
	"context"
	"errors"
	"github.com/syralon/syerrors"
)

type Middleware func(next Handler) Handler

func Recovery() Middleware {
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, topic string, data []byte) (err error) {
			defer func() {
				err = errors.Join(err, syerrors.Recovery(recover()))
			}()
			return next.Handle(ctx, topic, data)
		})
	}
}
