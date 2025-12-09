package events

import (
	"context"
	"errors"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"time"
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

type EventOption func(e *Event)

func WithDelay(delay time.Duration) EventOption {
	return func(e *Event) {
		e.Delay = durationpb.New(delay)
	}
}

func NewEventFromProto(ctx context.Context, topic string, m proto.Message, options ...EventOption) (*Event, error) {
	md := make(map[string]string)
	if outgoingMD, ok := metadata.FromOutgoingContext(ctx); ok {
		for k := range outgoingMD {
			md[k] = strings.Join(outgoingMD[k], ",")
		}
	}
	body, err := proto.Marshal(m)
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
	for _, option := range options {
		option(ev)
	}
	return ev, nil
}

func NewEventsFromProto(ctx context.Context, topic string, msgs ...proto.Message) ([]*Event, error) {
	md := make(map[string]string)
	if outgoingMD, ok := metadata.FromOutgoingContext(ctx); ok {
		for k := range outgoingMD {
			md[k] = strings.Join(outgoingMD[k], ",")
		}
	}
	var events = make([]*Event, 0, len(msgs))
	for _, m := range msgs {
		body, err := proto.Marshal(m)
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
