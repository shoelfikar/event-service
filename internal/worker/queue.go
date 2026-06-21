package worker

import (
	"context"
	"encoding/json"

	"github.com/hibiken/asynq"

	"github.com/streamforge/event-service/internal/model"
)

// TaskTypeEvent is the asynq task type for a single enriched event.
const TaskTypeEvent = "event:process"

// Enqueuer is the producer side used by the ingest handler.
type Enqueuer struct {
	client *asynq.Client
}

func NewEnqueuer(client *asynq.Client) *Enqueuer { return &Enqueuer{client: client} }

// Enqueue submits one enriched event as a durable job. asynq applies its
// default retry/backoff; jobs survive process restarts (stored in Redis).
func (e *Enqueuer) Enqueue(ctx context.Context, ev model.ProcessedEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = e.client.EnqueueContext(ctx, asynq.NewTask(TaskTypeEvent, payload))
	return err
}

// HandlerFunc adapts a Processor into an asynq handler for TaskTypeEvent.
func (p *Processor) HandlerFunc() asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var ev model.ProcessedEvent
		if err := json.Unmarshal(t.Payload(), &ev); err != nil {
			// Non-retryable: malformed payload.
			return asynq.SkipRetry
		}
		return p.Handle(ctx, ev)
	}
}
