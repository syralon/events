package events

import (
	"context"
	"google.golang.org/protobuf/proto"
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

type ProtoEventProducer struct {
	topic    string
	producer EventProducer
}

func NewProtoEventProducer(topic string, producer EventProducer) *ProtoEventProducer {
	return &ProtoEventProducer{topic: topic, producer: producer}
}

func (e *ProtoEventProducer) build(ctx context.Context, data ...proto.Message) ([]*Event, error) {
	return NewEventsFromProto(ctx, e.topic, data...)
}

func (e *ProtoEventProducer) Write(ctx context.Context, data ...proto.Message) error {
	events, err := NewEventsFromProto(ctx, e.topic, data...)
	if err != nil {
		return err
	}
	return e.producer.Write(ctx, events...)
}

func (e *ProtoEventProducer) WriteTx(ctx context.Context, data ...proto.Message) (Commiter, error) {
	events, err := NewEventsFromProto(ctx, e.topic, data...)
	if err != nil {
		return nil, err
	}
	return e.producer.WriteTx(ctx, events...)
}

func (e *ProtoEventProducer) Topic() string { return e.topic }

func (e *ProtoEventProducer) Mixing() *Mixing {
	return &Mixing{producer: e.producer}
}

type Mixing struct {
	producer EventProducer
	events   []*Event
}

func (m *Mixing) Add(ctx context.Context, topic string, data ...proto.Message) error {
	events, err := NewEventsFromProto(ctx, topic, data...)
	if err != nil {
		return err
	}
	m.events = append(m.events, events...)
	return nil
}

func (m *Mixing) Write(ctx context.Context) error {
	return m.producer.Write(ctx, m.events...)
}

func (m *Mixing) WriteTx(ctx context.Context) (Commiter, error) {
	return m.producer.WriteTx(ctx, m.events...)
}
