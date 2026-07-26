package task

import (
	"context"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/db"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
)

/*
performMaintenance process framework maintenance

	@param ctx context.Context - execution context
*/
func (s *schedulerImpl) performMaintenance(ctx context.Context) error {
	logTags := s.GetLogTagsForContext(ctx)

	// ------------------------------------------------------------------------------------
	// Process the tasks pending scheduling or cancellation

	var tasksToProcess []models.Task
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			var err error
			tasksToProcess, err = dbClient.ListTasks(dbCtx, db.TaskQueryFilter{
				TaskStates: []models.TaskStateENUM{
					models.TaskStatePending, models.TaskStateCancelling,
				},
			})
			if err != nil {
				return models.NewPersistenceError(
					"failed to list pending and cancelling tasks", err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskMaintenanceError("periodic maintenance failed", dbErr, true)
	}

	for _, oneTask := range tasksToProcess {
		switch oneTask.TaskState {
		case models.TaskStatePending:
			// New task pending scheduling
			if err := s.processNewPendingTask(ctx, oneTask.ID); err != nil {
				log.WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf("Maintenance failed to process pending task %s:\n%+v", oneTask.ID, err)
				continue
			}

		case models.TaskStateCancelling:
			// Task being cancelled
			if err := s.processCancelTask(ctx, oneTask.ID, time.Now().UTC()); err != nil {
				log.WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf("Maintenance failed to cancel task %s:\n%+v", oneTask.ID, err)
				continue
			}
		}
	}

	// ------------------------------------------------------------------------------------
	// Process the tasks which have timed out

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			var err error
			tasksToProcess, err = dbClient.ListTasks(dbCtx, db.TaskQueryFilter{
				TaskStates:     []models.TaskStateENUM{models.TaskStateActive},
				TargetDeadline: goutils.GetTypedPtr(time.Now().UTC()),
			})
			if err != nil {
				return models.NewPersistenceError(
					"failed to list timed out tasks", err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskMaintenanceError("periodic maintenance failed", dbErr, true)
	}

	for _, oneTask := range tasksToProcess {
		if err := s.processTaskTimeout(ctx, oneTask.ID, time.Now().UTC()); err != nil {
			log.WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf("Maintenance failed to process timed out task %s:\n%+v", oneTask.ID, err)
			continue
		}
	}

	// ------------------------------------------------------------------------------------
	// Process the task execution instances that completed or failed

	var execInstances []models.TaskExecution
	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			var err error
			execInstances, err = dbClient.ListAllExecutions(dbCtx, db.TaskExecutionQueryFilter{
				ExecStates: []models.TaskExecutionStateENUM{
					models.TaskExecutionStateProcessed, models.TaskExecutionStateFailed,
				},
			})
			if err != nil {
				return models.NewPersistenceError(
					"failed to list failed and completed task execution instances", err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskMaintenanceError("periodic maintenance failed", dbErr, true)
	}

	for _, oneInstance := range execInstances {
		switch oneInstance.ExecutionState {
		case models.TaskExecutionStateProcessed:
			if err := s.processTaskExecutionComplete(
				ctx, oneInstance.ID, oneInstance.UpdatedAt,
			); err != nil {
				log.WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf(
						"Maintenance failed to process completion of execution instance %s:\n%+v",
						oneInstance.ID,
						err,
					)
				continue
			}

		case models.TaskExecutionStateFailed:
			if err := s.processTaskExecutionFailed(
				ctx, oneInstance.ID, oneInstance.UpdatedAt,
			); err != nil {
				log.WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Errorf(
						"Maintenance failed to process failure of execution instance %s:\n%+v",
						oneInstance.ID,
						err,
					)
				continue
			}
		}
	}

	// ------------------------------------------------------------------------------------
	// Process the task execution instances that are scheduled to start

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			var err error
			execInstances, err = dbClient.ListAllExecutions(dbCtx, db.TaskExecutionQueryFilter{
				ExecStates:  []models.TaskExecutionStateENUM{models.TaskExecutionStateScheduled},
				TargetStart: goutils.GetTypedPtr(time.Now().UTC()),
			})
			if err != nil {
				return models.NewPersistenceError(
					"failed to list scheduled task execution instances", err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskMaintenanceError("periodic maintenance failed", dbErr, true)
	}

	for _, oneInstance := range execInstances {
		if err := s.processTaskExecutionStarting(
			ctx, oneInstance.ID, oneInstance.UpdatedAt,
		); err != nil {
			log.WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Maintenance failed to start execution instance %s:\n%+v", oneInstance.ID, err,
				)
			continue
		}
	}

	// ------------------------------------------------------------------------------------
	// Process the task execution instances that have timed out

	if dbErr := s.persistence.UseDatabaseInTransaction(
		ctx, func(dbCtx context.Context, dbClient db.Database) error {
			var err error
			execInstances, err = dbClient.ListAllExecutions(dbCtx, db.TaskExecutionQueryFilter{
				ExecStates: []models.TaskExecutionStateENUM{
					models.TaskExecutionStateDefined,
					models.TaskExecutionStateScheduled,
					models.TaskExecutionStateEnqueued,
					models.TaskExecutionStateAcquired,
					models.TaskExecutionStateProcessing,
				},
				TargetDeadline: goutils.GetTypedPtr(time.Now().UTC()),
			})
			if err != nil {
				return models.NewPersistenceError(
					"failed to list timed out task execution instances", err, true,
				)
			}
			return nil
		},
	); dbErr != nil {
		return models.NewTaskMaintenanceError("periodic maintenance failed", dbErr, true)
	}

	for _, oneInstance := range execInstances {
		if err := s.processTaskExecutionTimedOut(ctx, oneInstance.ID, time.Now().UTC()); err != nil {
			log.WithError(err).
				WithFields(goutils.UpdateCodePositionInTags(logTags)).
				Errorf(
					"Maintenance failed to process timeout of execution instance %s:\n%+v",
					oneInstance.ID,
					err,
				)
			continue
		}
	}

	return nil
}
