package events

import (
	"context"
	"errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	MaxBatchSize = 1000
)

var ErrBatchSizeUpToLimit = errors.New("events batch size up to limit")

type Events []*Event

func (e Events) Sift() map[string][]*Event {
	m := make(map[string][]*Event)
	for _, event := range e {
		m[event.GetTopic()] = append(m[event.GetTopic()], event)
	}
	return m
}

func NewEvents(ctx context.Context, topic string, marshaler func(any) ([]byte, error), msgs ...any) ([]*Event, error) {
	md := OutgoingMetadata(ctx)
	var events = make([]*Event, 0, len(msgs))
	for _, m := range msgs {
		body, err := marshaler(m)
		if err != nil {
			return nil, err
		}
		ev := &Event{
			Topic:     topic,
			Metadata:  md,
			Body:      body,
			Delay:     nil,
			Timestamp: timestamppb.Now(),
		}
		events = append(events, ev)
	}
	return events, nil
}
