package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// newNotifyTestScheduler build a white-box schedulerImpl for driving onNotification: only the queue
// sender (the adapter does no DB work) and the ipcName used as the message sender.
func newNotifyTestScheduler(ipcSender *mockcommon.IPCMessageSend) *schedulerImpl {
	return &schedulerImpl{
		Component: goutils.Component{LogTags: log.Fields{"module": "workflow"}},
		wg:        &sync.WaitGroup{},
		ipcName:   "workflow-scheduler",
		ipcSender: ipcSender,
	}
}

// notificationOfType build a task-subject NotificationEvent of the given event type for taskID.
func notificationOfType(
	eventType models.SystemEventTypeENUM, taskID string,
) models.NotificationEvent {
	subjectType := "task"
	return models.NotificationEvent{
		ID:          ulid.Make().String(),
		EventType:   eventType,
		SubjectType: &subjectType,
		SubjectID:   &taskID,
	}
}

func TestMapTaskEventToStepState(t *testing.T) {
	assert := assert.New(t)

	// Every terminal task event maps to its step outcome.
	mapped := map[models.SystemEventTypeENUM]models.WorkflowStepStateENUM{
		models.SystemEventTypeCompleteTask:     models.WorkflowStepStateComplete,
		models.SystemEventTypeFailedTask:       models.WorkflowStepStateFailed,
		models.SystemEventTypeEngineFailedTask: models.WorkflowStepStateFailed,
		models.SystemEventTypeTimedOutTask:     models.WorkflowStepStateTimedOut,
		models.SystemEventTypeCancelledTask:    models.WorkflowStepStateCancelled,
	}
	for eventType, want := range mapped {
		got, ok := mapTaskEventToStepState(eventType)
		assert.True(ok, "expected %s to be actionable", eventType)
		assert.Equal(want, got, "unexpected mapping for %s", eventType)
	}

	// Non-terminal / irrelevant types are not actionable.
	for _, eventType := range []models.SystemEventTypeENUM{
		models.SystemEventTypeActivateTask,
		models.SystemEventTypeDeleteTask,
		models.SystemEventTypeDefineWorkflow,
		models.SystemEventTypeInvalidTaskIPCMessage,
		"SOME_UNKNOWN_TYPE",
	} {
		got, ok := mapTaskEventToStepState(eventType)
		assert.False(ok, "expected %s to be non-actionable", eventType)
		assert.Empty(got)
	}
}

func TestOnNotification(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	utCtx := context.Background()

	t.Run("actionable event enqueues a task-keyed step task update", func(t *testing.T) {
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newNotifyTestScheduler(mockSender)

		taskID := ulid.Make().String()
		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.MatchedBy(func(m goutilsRedis.QueueMessageEnvelope) bool {
				typed, ok := m.(models.IPCMessageWorkflowStepTaskUpdate)
				return ok &&
					typed.Type == models.IPCMsgTypeWFStepTaskUpdate &&
					typed.TaskID == taskID &&
					typed.NewStepState == models.WorkflowStepStateComplete
			})).
			Return(nil)

		s.onNotification(utCtx, notificationOfType(models.SystemEventTypeCompleteTask, taskID))
	})

	t.Run("non-actionable event is dropped without enqueue", func(t *testing.T) {
		// mockSender with no EXPECT: any EnqueueMessage call fails the test.
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newNotifyTestScheduler(mockSender)

		s.onNotification(
			utCtx, notificationOfType(models.SystemEventTypeActivateTask, ulid.Make().String()),
		)
	})

	t.Run("missing subject task ID is dropped without enqueue", func(t *testing.T) {
		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newNotifyTestScheduler(mockSender)

		event := notificationOfType(models.SystemEventTypeCompleteTask, "")
		event.SubjectID = nil // no subject at all
		s.onNotification(utCtx, event)
	})

	t.Run("enqueue failure is logged, not fatal", func(t *testing.T) {
		assert := assert.New(t)

		mockSender := mockcommon.NewIPCMessageSend(t)
		s := newNotifyTestScheduler(mockSender)

		mockSender.EXPECT().
			EnqueueMessage(mock.Anything, mock.Anything).
			Return(fmt.Errorf("simulated failure"))

		// Callback is void; a failed enqueue must not panic.
		assert.NotPanics(func() {
			s.onNotification(
				utCtx, notificationOfType(models.SystemEventTypeFailedTask, ulid.Make().String()),
			)
		})
	})
}
