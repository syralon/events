package events

import (
	"context"
)

type Commiter interface {
	Commit(ctx context.Context) error
}

type txn struct {
	ids    []int64
	client EventServiceClient
}

func (t *txn) Commit(ctx context.Context) error {
	_, err := t.client.Commit(ctx, &CommitEventsRequest{Ids: t.ids})
	return err
}

type EventProducer interface {
	Write(ctx context.Context, events ...*Event) error
	WriteTx(ctx context.Context, events ...*Event) (Commiter, error)
}

type client struct {
	client EventServiceClient
}

func (c *client) Write(ctx context.Context, events ...*Event) error {
	if len(events) > MaxBatchSize {
		return ErrBatchSizeUpToLimit
	}
	_, err := c.client.Write(ctx, &WriteEventsRequest{Events: events})
	if err != nil {
		return err
	}
	return nil
}

func (c *client) WriteTx(ctx context.Context, events ...*Event) (Commiter, error) {
	if len(events) > MaxBatchSize {
		return nil, ErrBatchSizeUpToLimit
	}
	resp, err := c.client.Prepare(ctx, &PrepareEventsRequest{Events: events})
	if err != nil {
		return nil, err
	}
	return &txn{ids: resp.GetIds(), client: c.client}, nil
}

func NewEventProducer(ec EventServiceClient) EventProducer {
	return &client{client: ec}
}
