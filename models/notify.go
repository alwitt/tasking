package models

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
)

// NotificationEvent the notification payload broadcast by the notification producer over
// Redis pub/sub. It is the full audit event (id, type, created_at, typed metadata) enriched
// with the derived routing keys (creator, subject). Serialized to JSON and carried as a
// goutilsRedis.QueueMessageEnvelope; subscribers deserialize it in their handler.
//
// Optional routing keys are pointers so an absent creator/subject serializes as omitted
// rather than an ambiguous empty string.
type NotificationEvent struct {
	// ID audit event ID (subscribers dedupe on this — production is at-least-once)
	ID string `json:"id" validate:"required"`
	// EventType system event type
	EventType SystemEventTypeENUM `json:"type" validate:"required,system_event_type"`
	// CreatedAt when the audit event was recorded
	CreatedAt time.Time `json:"created_at"`
	// Creator opaque identity of the event's creator, when the event has one
	Creator *string `json:"creator,omitempty"`
	// SubjectType type of the subject the event is about (e.g. "task"), when derivable
	SubjectType *string `json:"subject_type,omitempty"`
	// SubjectID ID of the subject the event is about, when derivable
	SubjectID *string `json:"subject_id,omitempty"`
	// Metadata the event's typed metadata, as recorded on the audit row
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// StringPayload return its payload as a string; implements goutilsRedis.QueueMessageEnvelope
func (e NotificationEvent) StringPayload() (string, error) {
	t, err := json.Marshal(&e)
	return string(t), err
}

// ParseMetadata parse the event's metadata into its typed struct based on the event type,
// mirroring SystemEventAudit.ParseMetadata (they share the same underlying parser).
func (e NotificationEvent) ParseMetadata(validator *validator.Validate) (interface{}, error) {
	return parseSystemEventMetadata(e.EventType, e.Metadata, validator)
}

// ======================================================================================
// Notification channel name helpers
//
// The channel name is only a routing selector; the payload is identical across channels.
// Naming is centralized here so producers and subscribers agree on the wire convention.

// notifyChannelPrefix common prefix for all notification pub/sub channels
const notifyChannelPrefix = "notify"

// BuildNotifyFirehoseChannelName the firehose channel carrying every event: `notify:all`
func BuildNotifyFirehoseChannelName() string {
	return strings.Join([]string{notifyChannelPrefix, "all"}, ":")
}

// BuildNotifyTypeChannelName the per-event-type channel: `notify:type:<type>`
func BuildNotifyTypeChannelName(eventType SystemEventTypeENUM) string {
	return strings.Join([]string{notifyChannelPrefix, "type", string(eventType)}, ":")
}

// BuildNotifyCreatorChannelName the per-creator channel: `notify:creator:<creator>`
func BuildNotifyCreatorChannelName(creator string) string {
	return strings.Join([]string{notifyChannelPrefix, "creator", creator}, ":")
}

// BuildNotifyCreatorTypeChannelName the creator∩type channel:
// `notify:creator:<creator>:type:<type>`
func BuildNotifyCreatorTypeChannelName(creator string, eventType SystemEventTypeENUM) string {
	return strings.Join(
		[]string{notifyChannelPrefix, "creator", creator, "type", string(eventType)}, ":",
	)
}

// BuildNotifySubjectChannelName the per-subject channel:
// `notify:subject:<subject-type>:<subject-id>`
func BuildNotifySubjectChannelName(subjectType, subjectID string) string {
	return strings.Join(
		[]string{notifyChannelPrefix, "subject", subjectType, subjectID}, ":",
	)
}
