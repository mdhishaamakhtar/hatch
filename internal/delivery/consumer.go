package delivery

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// RunConsumer is G1: poll emails.due, hand each Batch to G2, and commit the
// offsets only after G2 acks — the at-least-once guarantee. Redis idempotency in
// G2 prevents duplicate sends if the worker crashes before the commit. batchC is
// buffered (1) for one Batch of lookahead; ackC is unbuffered so the commit can
// never race ahead of processing.
//
// pollCtx and workCtx are deliberately separate. On SIGTERM only pollCtx is
// cancelled, which stops fetching new work; the batch already in flight runs to
// completion (and gets its offsets committed) under workCtx, which the caller
// cancels after the drain budget. Sharing one context would abort a send
// mid-flight and leave the row stranded in `processing`.
func RunConsumer(pollCtx, workCtx context.Context, lg *zap.Logger, cl *kgo.Client, batchSize int, batchC chan<- Batch, ackC <-chan struct{}) {
	for pollCtx.Err() == nil {
		fetches := cl.PollRecords(pollCtx, batchSize)
		if fetches.IsClientClosed() {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			if pollCtx.Err() != nil {
				return // shutdown
			}
			for _, e := range errs {
				lg.Warn("kafka fetch error", zap.String("topic", e.Topic), zap.Error(e.Err))
			}
			continue
		}

		recs := make([]*kgo.Record, 0, batchSize)
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) == 0 {
			continue
		}

		// From here on the batch is owned by workCtx: once handed over it must be
		// allowed to finish and commit, even though polling has stopped.
		select {
		case batchC <- Batch{recs: recs}:
		case <-workCtx.Done():
			return
		}
		select {
		case <-ackC:
		case <-workCtx.Done():
			return
		}

		if err := cl.CommitRecords(workCtx, recs...); err != nil {
			lg.Error("kafka commit failed", zap.Error(err), zap.Int("records", len(recs)))
		}
	}
}
