package task

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	goutilsRedis "github.com/alwitt/goutils/redis"
	mockcommon "github.com/alwitt/tasking/mocks/common"
	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// recordedEnvelope a minimal goutilsRedis.QueueMessageEnvelope used to stand in for
// the original IPC message recorded against an execution instance.
type recordedEnvelope struct {
	payload string
}

// StringPayload return its payload as a string
func (r recordedEnvelope) StringPayload() (string, error) {
	return r.payload, nil
}

// TestOnTaskComplete validates the executor completion callback: it reports the
// correct outcome to the scheduler, removes the recorded IPC message from the queue
// buffer, and cleans up the recorded-message pool entry. This test lives in the
// `task` package so it can seed the unexported execInstanceOriginalIPCMsg map, the
// only way to reach the recorded-envelope branch (which ProcessOneIPCRequest owns).
func TestOnTaskComplete(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()
	simErr := fmt.Errorf("simulated failure")

	nonRetryable := models.TaskFailureDispositionNonRetryable

	type testCase struct {
		name string
		// taskErr the error handed to OnTaskComplete (nil means success).
		taskErr error
		// seedPool whether to seed the recorded-message pool for the instance.
		seedPool bool
		// expectType the IPC message type expected on the scheduler queue.
		expectType models.IPCMessageTypeEnum
		// expectDisposition the disposition expected on an EXECUTE_FAILED message (nil otherwise).
		expectDisposition *models.TaskFailureDispositionENUM
		// enqueueErr the error the scheduler sender returns.
		enqueueErr error
		// deleteErr the error the buffer delete returns.
		deleteErr error
	}

	cases := []testCase{
		{
			name:       "success with recorded msg",
			taskErr:    nil,
			seedPool:   true,
			expectType: models.IPCMsgTypeExecuteSucceeded,
		},
		{
			name:       "failure with recorded msg",
			taskErr:    simErr,
			seedPool:   true,
			expectType: models.IPCMsgTypeExecuteFailed,
		},
		{
			// A plain (recoverable) failure reports EXECUTE_FAILED with no disposition,
			// which the scheduler treats as retryable.
			name:       "recoverable failure has no disposition",
			taskErr:    models.NewTaskExecutionError("boom", simErr, true),
			seedPool:   true,
			expectType: models.IPCMsgTypeExecuteFailed,
		},
		{
			// A NonRecoverableError reports EXECUTE_FAILED with a NON_RETRYABLE disposition.
			name:              "non-recoverable failure carries disposition",
			taskErr:           models.NewTaskExecutionError("boom", models.NewNonRecoverableError("permanent", nil, true), true),
			seedPool:          true,
			expectType:        models.IPCMsgTypeExecuteFailed,
			expectDisposition: &nonRetryable,
		},
		{
			// An engine-level failure (e.g. missing processor) reports ENGINE_FAILED.
			name:       "engine failure reports engine failed",
			taskErr:    models.NewTaskExecutorError("missing processor", nil, true),
			seedPool:   true,
			expectType: models.IPCMsgTypeEngineFailed,
		},
		{
			name:       "success without recorded msg",
			taskErr:    nil,
			seedPool:   false,
			expectType: models.IPCMsgTypeExecuteSucceeded,
		},
		{
			name:       "enqueue error is swallowed",
			taskErr:    nil,
			seedPool:   true,
			expectType: models.IPCMsgTypeExecuteSucceeded,
			enqueueErr: simErr,
		},
		{
			name:       "delete error is swallowed",
			taskErr:    nil,
			seedPool:   true,
			expectType: models.IPCMsgTypeExecuteSucceeded,
			deleteErr:  simErr,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			instanceID := ulid.Make().String()

			mockSender := mockcommon.NewIPCMessageSend(t)
			queueReceiver := mockcommon.NewIPCMessageReceive(t)

			r := &receiverImpl{
				Component:                  goutils.Component{LogTags: log.Fields{"module": "task"}},
				config:                     models.TaskReceiverConfig{Name: "unit-test-worker"},
				schedulerIPCSender:         mockSender,
				ipcMsgPoolLock:             &sync.Mutex{},
				execInstanceOriginalIPCMsg: map[string]goutilsRedis.QueueMessageEnvelope{},
			}

			var recorded goutilsRedis.QueueMessageEnvelope
			if tc.seedPool {
				recorded = recordedEnvelope{payload: "recorded-" + instanceID}
				r.execInstanceOriginalIPCMsg[instanceID] = recorded
			}

			// The scheduler is notified with the outcome-appropriate message type.
			mockSender.EXPECT().
				EnqueueMessage(mock.Anything, mock.Anything).
				Run(func(_ context.Context, msg goutilsRedis.QueueMessageEnvelope) {
					execMsg, ok := msg.(models.IPCMessageExecuteInstance)
					assert.True(ok, "expected IPCMessageExecuteInstance, got %T", msg)
					assert.Equal(tc.expectType, execMsg.Type)
					assert.Equal(instanceID, execMsg.InstanceID)
					if tc.expectDisposition == nil {
						assert.Nil(execMsg.Disposition)
					} else {
						assert.NotNil(execMsg.Disposition)
						if execMsg.Disposition != nil {
							assert.Equal(*tc.expectDisposition, *execMsg.Disposition)
						}
					}
				}).
				Return(tc.enqueueErr)

			// The recorded message (or nil when no entry was recorded) is deleted from
			// the queue buffer.
			var expectDeleteArg interface{}
			if tc.seedPool {
				expectDeleteArg = recorded
			} else {
				expectDeleteArg = nil
			}
			queueReceiver.EXPECT().
				DeleteBufferedMessage(mock.Anything, expectDeleteArg).
				Return(tc.deleteErr)

			// Must not panic or propagate regardless of enqueue/delete errors.
			r.onTaskComplete(utCtx, queueReceiver, instanceID, tc.taskErr, time.Now().UTC())

			// The recorded-message pool entry is always cleared after completion.
			_, stillPresent := r.execInstanceOriginalIPCMsg[instanceID]
			assert.False(stillPresent, "expected recorded pool entry to be removed")
		})
	}
}
