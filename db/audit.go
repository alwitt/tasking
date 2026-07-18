// Package db - database controllers for system persistence
package db

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/models"
	"github.com/oklog/ulid/v2"
	"gorm.io/datatypes"
)

// defineNewSystemEvent record a new system event
func (c *databaseImpl) defineNewSystemEvent(
	_ context.Context, eventType models.SystemEventTypeENUM, metadata interface{},
) (models.SystemEventAudit, error) {
	if err := c.validator.Struct(metadata); err != nil {
		return models.SystemEventAudit{}, goutils.NewValidationError(
			fmt.Sprintf("new system event '%s' metadata entry is not valid", eventType), err, true,
		)
	}

	metadataStr, _ := json.Marshal(&metadata)

	newEntry := systemEventAuditEntry{
		SystemEventAudit: models.SystemEventAudit{
			ID:        ulid.Make().String(),
			EventType: eventType,
			Metadata:  datatypes.JSON(metadataStr),
		},
	}

	if err := c.validator.Struct(&newEntry); err != nil {
		return models.SystemEventAudit{}, goutils.NewValidationError(
			fmt.Sprintf("new system event '%s' entry is not valid", eventType), err, true,
		)
	}

	if tmp := c.db.Create(&newEntry); tmp.Error != nil {
		return models.SystemEventAudit{}, models.NewSQLError(
			fmt.Sprintf("new system event '%s' insert failed", eventType), tmp.Error, true,
		)
	}

	return newEntry.SystemEventAudit, nil
}

/*
RecordInvalidTaskIPCMessage record an audit event for a task IPC message that could not be
processed (unreadable, unparsable, or of an unsupported/unknown type).

	@param ctx context.Context - execution context
	@param receiver string - name of the IPC receiver that rejected the message
	@param rawMessage string - the raw message payload, if it was readable
	@param reason string - human-readable reason the message was rejected
*/
func (c *databaseImpl) RecordInvalidTaskIPCMessage(
	ctx context.Context, receiver, rawMessage, reason string,
) error {
	if _, err := c.defineNewSystemEvent(
		ctx,
		models.SystemEventTypeInvalidTaskIPCMessage,
		&models.SystemEventInvalidTaskIPCMessage{
			Receiver: receiver, RawMessage: rawMessage, Reason: reason,
		},
	); err != nil {
		return err
	}
	return nil
}

/*
RecordTaskEngineFailure record an audit event for a task whose execution instance the core
task engine failed to operate on (e.g. the receiver could not claim it, or could not submit
it to the executor).

	@param ctx context.Context - execution context
	@param taskID string - ID of the task that was failed
	@param instanceID string - ID of the execution instance the engine failed to operate on
	@param reason string - human-readable reason the engine reported the failure
*/
func (c *databaseImpl) RecordTaskEngineFailure(
	ctx context.Context, taskID, instanceID, reason string,
) error {
	if _, err := c.defineNewSystemEvent(
		ctx,
		models.SystemEventTypeEngineFailedTask,
		&models.SystemEventEngineFailedTask{
			TaskID: taskID, InstanceID: instanceID, Reason: reason,
		},
	); err != nil {
		return err
	}
	return nil
}

/*
ListSystemEvents list captured system events

	@param ctx context.Context - execution context
	@param filters SystemEventQueryFilter - entry listing filter
	@return list of system events
*/
func (c *databaseImpl) ListSystemEvents(
	_ context.Context, filters SystemEventQueryFilter,
) ([]models.SystemEventAudit, error) {
	if err := c.validator.Struct(&filters); err != nil {
		return nil, goutils.NewValidationError("system event query filter is not valid", err, true)
	}

	query := c.db.Model(&systemEventAuditEntry{})

	if len(filters.EventTypes) > 0 {
		query = query.Where("type in ?", filters.EventTypes)
	}

	if filters.EventsAfter != nil {
		query = query.Where("created_at >= ?", *filters.EventsAfter)
	}
	if filters.EventsBefore != nil {
		query = query.Where("created_at <= ?", *filters.EventsBefore)
	}

	if filters.Limit != nil {
		query = query.Limit(*filters.Limit)
	}
	if filters.Offset != nil {
		query = query.Offset(*filters.Offset)
	}

	query = query.Order("created_at")

	var entries []systemEventAuditEntry
	if tmp := query.Find(&entries); tmp.Error != nil {
		return nil, models.NewSQLError("failed to list captured system events", tmp.Error, true)
	}

	result := []models.SystemEventAudit{}
	for _, entry := range entries {
		result = append(result, entry.SystemEventAudit)
	}

	return result, nil
}
