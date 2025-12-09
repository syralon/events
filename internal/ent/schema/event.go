package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/syralon/events/internal/global/snowflake"
	"time"
)

// Event holds the schema definition for the Event entity.
type Event struct {
	ent.Schema
}

// Fields of the Event.
func (Event) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").DefaultFunc(snowflake.Next),
		field.String("topic"),
		field.String("trace_id").Optional(),
		field.Bytes("metadata").Optional(),
		field.Bytes("body"),
		field.Int64("delay").Default(0),
		field.Time("expected_at").Default(time.Now),
		field.Time("created_at").Default(time.Now),
		field.Time("updated_at").Default(time.Now).UpdateDefault(time.Now),
	}
}
func (Event) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("topic"),
	}
}

// Edges of the Event.
func (Event) Edges() []ent.Edge {
	return nil
}
