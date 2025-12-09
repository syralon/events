package events

import (
	"context"
	"encoding/json"
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

type Writer interface {
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

func NewWriter(ec EventServiceClient) Writer {
	return &client{client: ec}
}

type writer struct {
	w         Writer
	marshaler func(v any) ([]byte, error)
}

type EventWriter struct {
	topic string

	writer
}

type EventWriteOption func(*EventWriter)

func WithMarshaler(fn func(v any) ([]byte, error)) EventWriteOption {
	return func(e *EventWriter) {
		e.marshaler = fn
	}
}

func ProtoMarshal(v any) ([]byte, error) {
	if m, ok := v.(proto.Message); ok {
		return proto.Marshal(m)
	}
	return json.Marshal(v)
}

func WithProtoMarshaler() EventWriteOption {
	return func(e *EventWriter) {
		e.marshaler = ProtoMarshal
	}
}

func NewEventWriter(topic string, w Writer, opts ...EventWriteOption) *EventWriter {
	e := &EventWriter{topic: topic, writer: writer{w: w, marshaler: json.Marshal}}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func (e *EventWriter) Topic() string { return e.topic }

func (e *EventWriter) Write(ctx context.Context, evs ...any) error {
	events, err := NewEvents(ctx, e.topic, e.marshaler, evs...)
	if err != nil {
		return err
	}
	return e.w.Write(ctx, events...)
}

func (e *EventWriter) WriteTx(ctx context.Context, evs ...any) (Commiter, error) {
	events, err := NewEvents(ctx, e.topic, e.marshaler, evs...)
	if err != nil {
		return nil, err
	}
	return e.w.WriteTx(ctx, events...)
}

func (e *EventWriter) Mixing() *Mixing {
	return &Mixing{writer: e.writer}
}

type Mixing struct {
	writer

	events []*Event
	err    error
}

func (m *Mixing) Add(ctx context.Context, topic string, evs ...any) *Mixing {
	if m.err != nil {
		return m
	}
	events, err := NewEvents(ctx, topic, m.marshaler, evs...)
	if err != nil {
		m.err = err
		return m
	}
	m.events = append(m.events, events...)
	return m
}

func (m *Mixing) Write(ctx context.Context) error {
	if m.err != nil {
		return m.err
	}
	return m.w.Write(ctx, m.events...)
}

func (m *Mixing) WriteTx(ctx context.Context) (Commiter, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.w.WriteTx(ctx, m.events...)
}
