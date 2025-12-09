package events

import "errors"

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
