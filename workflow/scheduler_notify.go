package workflow

import (
	"context"
	"time"

	"github.com/alwitt/goutils"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
)

/*
mapTaskEventToStepState maps a terminal task event type to the step outcome it drives, and reports
whether the event is one the workflow engine acts on.

The engine's creator channel carries every task event the engine created; only the five terminal
types map to a step outcome (workflow/DESIGN.md §256-258). Everything else - ACTIVATE_TASK, any
non-terminal or non-task event - returns ok=false and is dropped by the adapter. ENGINE_FAILED_TASK
(the engine could not run the attempt) maps to FAILED, same as a task-execution failure.

	@param eventType models.SystemEventTypeENUM - the source task event type
	@returns the terminal step state it drives, and whether the event is relevant
*/
func mapTaskEventToStepState(
	eventType models.SystemEventTypeENUM,
) (models.WorkflowStepStateENUM, bool) {
	switch eventType {
	case models.SystemEventTypeCompleteTask:
		return models.WorkflowStepStateComplete, true
	case models.SystemEventTypeFailedTask, models.SystemEventTypeEngineFailedTask:
		return models.WorkflowStepStateFailed, true
	case models.SystemEventTypeTimedOutTask:
		return models.WorkflowStepStateTimedOut, true
	case models.SystemEventTypeCancelledTask:
		return models.WorkflowStepStateCancelled, true
	default:
		return "", false
	}
}

/*
onNotification the notify.NotificationCallback the scheduler subscribes with. It is the thin adapter
from a task-engine feedback notification into the scheduler queue (workflow/DESIGN.md §229-242).

It runs on the notify subscriber's single reader goroutine and MUST return promptly, so it does NO
DB work: it filters by event type, reads the task ID off the event, maps the event type to a
terminal step state, and enqueues a task-keyed Step Task Update onto the scheduler queue. The heavy
work - resolving task -> step and advancing the DAG - happens later on the single-threaded scheduler
worker (applyStepTaskUpdate -> applyStepExecutionUpdate).

Best-effort by design: an irrelevant event, a subject-less event, or a failed enqueue is logged and
dropped. A lost poke only delays a workflow; the maintenance sweep reconciles from the DB.

	@param ctx context.Context - the notify subscriber's working context
	@param event models.NotificationEvent - the received notification
*/
func (s *schedulerImpl) onNotification(ctx context.Context, event models.NotificationEvent) {
	logTags := s.GetLogTagsForContext(ctx)

	newStepState, ok := mapTaskEventToStepState(event.EventType)
	if !ok {
		// Not a terminal task event the engine acts on (e.g. ACTIVATE_TASK); drop.
		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Debugf("Ignoring notification %s of non-actionable type %s", event.ID, event.EventType)
		return
	}

	// For task events the subject is the task (subject:task:<taskID>); SubjectID is the task ID.
	if event.SubjectID == nil || *event.SubjectID == "" {
		log.WithFields(goutils.UpdateCodePositionInTags(logTags)).Warnf(
			"Dropping notification %s of type %s: missing subject task ID", event.ID, event.EventType,
		)
		return
	}
	taskID := *event.SubjectID

	if err := s.ipcSender.EnqueueMessage(
		ctx, models.PrepareIPCMsgWFStepTaskUpdate(s.ipcName, taskID, newStepState, time.Now().UTC()),
	); err != nil {
		// Best-effort fast path: a dropped poke is reconciled by the maintenance sweep. Log, not fail.
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Errorf("Failed to enqueue step task update for task %s", taskID)
	}
}
