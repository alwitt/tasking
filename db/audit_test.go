package db_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

// validatorInstance build a validator wired with the model validation macros, for parsing
// audit metadata read back from the store.
func validatorInstance(t *testing.T) *validator.Validate {
	v := validator.New()
	assert.Nil(t, models.RegisterWithValidator(v))
	return v
}

func TestAuditRecordTaskEngineFailure(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// Record an engine-failure audit event
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.RecordTaskEngineFailure(
				ctx,
				models.Task{ID: "unit-test-task-id", Creator: "unit-test-creator"},
				"unit-test-instance-id", "could not claim instance",
			)
		},
	))

	// Read it back and confirm the metadata round-tripped
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventTypes: []models.SystemEventTypeENUM{
					models.SystemEventTypeEngineFailedTask,
				},
			})
			if err != nil {
				return err
			}
			assert.Len(events, 1)
			assert.NotEmpty(events[0].ID)
			assert.Equal(models.SystemEventTypeEngineFailedTask, events[0].EventType)

			parsed, err := events[0].ParseMetadata(validatorInstance(t))
			assert.Nil(err)
			metadata, ok := parsed.(models.SystemEventEngineFailedTask)
			assert.True(ok, "expected SystemEventEngineFailedTask, got %T", parsed)
			assert.Equal("unit-test-task-id", metadata.TaskID)
			assert.Equal("unit-test-instance-id", metadata.InstanceID)
			assert.Equal("could not claim instance", metadata.Reason)
			assert.Equal("unit-test-creator", metadata.Creator)
			return nil
		},
	))
}

func TestAuditListSystemEvents(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// Empty store lists nothing (and returns a non-nil empty slice)
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{})
			if err != nil {
				return err
			}
			assert.NotNil(events)
			assert.Len(events, 0)
			return nil
		},
	))

	// Seed a mix of event types
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			if err := dbClient.RecordInvalidTaskIPCMessage(
				ctx, "scheduler", "payload-a", "unparsable message",
			); err != nil {
				return err
			}
			if err := dbClient.RecordInvalidTaskIPCMessage(
				ctx, "scheduler", "payload-b", "unreadable payload",
			); err != nil {
				return err
			}
			return dbClient.RecordTaskEngineFailure(
				ctx,
				models.Task{ID: "task-x", Creator: "creator-x"},
				"instance-x", "could not submit to executor",
			)
		},
	))

	// No filter lists them all
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{})
			if err != nil {
				return err
			}
			assert.Len(events, 3)
			return nil
		},
	))

	// EventTypes filter narrows to a single type
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventTypes: []models.SystemEventTypeENUM{
					models.SystemEventTypeEngineFailedTask,
				},
			})
			if err != nil {
				return err
			}
			assert.Len(events, 1)
			assert.Equal(models.SystemEventTypeEngineFailedTask, events[0].EventType)
			return nil
		},
	))

	// Limit + Offset paginate (results are ordered by id)
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			limit := 2
			firstPage, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				CommonListEntryQueryFilter: db.CommonListEntryQueryFilter{Limit: &limit},
			})
			if err != nil {
				return err
			}
			assert.Len(firstPage, 2)

			offset := 2
			secondPage, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				CommonListEntryQueryFilter: db.CommonListEntryQueryFilter{
					Limit: &limit, Offset: &offset,
				},
			})
			if err != nil {
				return err
			}
			assert.Len(secondPage, 1)
			// The pages must not overlap
			assert.NotEqual(firstPage[0].ID, secondPage[0].ID)
			assert.NotEqual(firstPage[1].ID, secondPage[0].ID)
			return nil
		},
	))
}

func TestAuditListSystemEventsTimeRange(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// Record an event
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.RecordTaskEngineFailure(
				ctx, models.Task{ID: "task-x", Creator: "creator-x"}, "instance-x", "boom",
			)
		},
	))

	// Read its own persisted CreatedAt and bracket the time-range filters around that. The
	// filters compare against the stored created_at, so deriving the cutoffs from it (rather
	// than from a wall clock captured in the test) keeps the assertions independent of the
	// column's timezone/precision.
	var recordedAt time.Time
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{})
			if err != nil {
				return err
			}
			assert.Len(events, 1)
			recordedAt = events[0].CreatedAt
			return nil
		},
	))
	assert.False(recordedAt.IsZero())

	before := recordedAt.Add(-time.Second)
	after := recordedAt.Add(time.Second)

	// EventsAfter a moment before the write returns it
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventsAfter: &before,
			})
			if err != nil {
				return err
			}
			assert.Len(events, 1)
			return nil
		},
	))

	// EventsAfter a moment past the write excludes it
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventsAfter: &after,
			})
			if err != nil {
				return err
			}
			assert.Len(events, 0)
			return nil
		},
	))

	// EventsBefore a moment past the write returns it
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventsBefore: &after,
			})
			if err != nil {
				return err
			}
			assert.Len(events, 1)
			return nil
		},
	))

	// EventsBefore a moment before the write excludes it
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventsBefore: &before,
			})
			if err != nil {
				return err
			}
			assert.Len(events, 0)
			return nil
		},
	))
}

func TestAuditListSystemEventsInvalidFilter(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// An unknown event type in the filter fails the `system_event_type` validation macro
	err := persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			_, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				EventTypes: []models.SystemEventTypeENUM{"NOT_A_REAL_EVENT_TYPE"},
			})
			return err
		},
	)
	assert.NotNil(err)
	var validationErr goutils.ValidationError
	assert.ErrorAs(err, &validationErr)
}

func TestAuditBroadcastMarker(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	testDB := fmt.Sprintf("/tmp/tasking_ut_%s.db", ulid.Make().String())
	log.WithField("db", testDB).Debug("Test database")

	persistence := getUnitTestPersistence(utCtx, t, testDB)

	// Seed three events
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			for i := 0; i < 3; i++ {
				if err := dbClient.RecordTaskEngineFailure(
					ctx,
					models.Task{ID: fmt.Sprintf("task-%d", i), Creator: "creator-x"},
					fmt.Sprintf("instance-%d", i), "boom",
				); err != nil {
					return err
				}
			}
			return nil
		},
	))

	// All three are not-yet-broadcast, ordered by id
	var allIDs []string
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				OnlyNotBroadcast: true,
			})
			if err != nil {
				return err
			}
			assert.Len(events, 3)
			for _, e := range events {
				assert.Nil(e.BroadcastAt)
				allIDs = append(allIDs, e.ID)
			}
			// ordered by id (ULID, lexicographically ascending)
			assert.True(allIDs[0] < allIDs[1] && allIDs[1] < allIDs[2])
			return nil
		},
	))

	// Empty input is a no-op
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.MarkSystemEventsBroadcast(ctx, nil, time.Now())
		},
	))

	// Stamp the first two
	stampAt := time.Now().UTC()
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.MarkSystemEventsBroadcast(ctx, allIDs[:2], stampAt)
		},
	))

	// Only the third remains not-yet-broadcast
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{
				OnlyNotBroadcast: true,
			})
			if err != nil {
				return err
			}
			assert.Len(events, 1)
			assert.Equal(allIDs[2], events[0].ID)
			return nil
		},
	))

	// Re-stamping an already-stamped event does not overwrite its broadcast_at (idempotency
	// guard). Stamp allIDs[0] again with a distinctly later time; its recorded time is unchanged.
	laterStamp := stampAt.Add(time.Hour)
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			return dbClient.MarkSystemEventsBroadcast(ctx, []string{allIDs[0]}, laterStamp)
		},
	))
	assert.Nil(persistence.UseDatabaseInTransaction(
		utCtx, func(ctx context.Context, dbClient db.Database) error {
			events, err := dbClient.ListSystemEvents(ctx, db.SystemEventQueryFilter{})
			if err != nil {
				return err
			}
			for _, e := range events {
				if e.ID == allIDs[0] {
					assert.NotNil(e.BroadcastAt)
					// still the original stamp, not laterStamp
					assert.WithinDuration(stampAt, *e.BroadcastAt, time.Second)
				}
			}
			return nil
		},
	))
}
