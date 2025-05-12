// Package batcher groups items in batches
// and calls the user-specified function on these batches.
package batcher

import (
	"sync"
	"time"
)

type Task[Req any, Res any] struct {
	Req    Req
	Res    Res
	doneCh chan error
}

func (task *Task[Req, Res]) Done(err error) {
	task.doneCh <- err
	close(task.doneCh)
}

// GenericBatcherTask groups items in batches and calls Func on them.
//
// See also BytesBatcher.
type GenericBatcherTask[Req any, Res any] struct {
	// Func is called by GenericBatcherTask when batch is ready to be processed.
	Func GenericBatcherTaskFunc[Task[Req, Res]]

	// Maximum batch size that will be passed to BatcherFunc.
	MaxBatchSize int

	// Maximum delay between Push() and BatcherFunc call.
	MaxDelay time.Duration

	// Maximum unprocessed items' queue size.
	QueueSize int

	// MaxConcurrency limits the number of concurrent goroutines processing batches
	// Default is 100
	MaxConcurrency int

	ch        chan *Task[Req, Res]
	doneCh    chan struct{}
	semaphore chan struct{}
}

// GenericBatcherTaskFunc is called by GenericBatcherTask when batch is ready to be processed.
//
// GenericBatcherTaskFunc must process the given batch before returning.
// It must not hold references to the batch after returning.
type GenericBatcherTaskFunc[T any] func(batch []*T)

// Start starts batch processing.
func (b *GenericBatcherTask[Req, Res]) Start() {
	if b.ch != nil {
		panic("batcher already started")
	}
	if b.Func == nil {
		panic("GenericBatcherTask.Func must be set")
	}

	if b.QueueSize <= 0 {
		b.QueueSize = 8 * 1024
	}
	if b.MaxBatchSize <= 0 {
		b.MaxBatchSize = 64 * 1024
	}
	if b.MaxDelay <= 0 {
		b.MaxDelay = time.Millisecond
	}
	if b.MaxConcurrency <= 0 {
		b.MaxConcurrency = 100
	}

	b.ch = make(chan *Task[Req, Res], b.QueueSize)
	b.doneCh = make(chan struct{})
	b.semaphore = make(chan struct{}, b.MaxConcurrency)

	go func() {
		processGenericBatchesTask(b.Func, b.ch, b.MaxBatchSize, b.MaxDelay, b.semaphore)
		close(b.doneCh)
	}()
}

// Stop stops batch processing.
func (b *GenericBatcherTask[Req, Res]) Stop() {
	if b.ch == nil {
		panic("BUG: forgot calling GenericBatcherTask.Start()?")
	}
	close(b.ch)
	<-b.doneCh
	b.ch = nil
	b.doneCh = nil
	b.semaphore = nil
}

// Do pushes new item into the batcher.
// Do waits for the queue to be unblocked when pushing a new dby item
func (b *GenericBatcherTask[Req, Res]) Do(req Req) (resp Res, err error) {
	if b.ch == nil {
		panic("BUG: forgot calling GenericBatcherTask.Start()?")
	}

	chv := errorChPool.Get()
	if chv == nil {
		chv = make(chan error, 1)
	}

	task := &Task[Req, Res]{
		Req:    req,
		doneCh: chv.(chan error)}

	select {
	case b.ch <- task:
		err = <-task.doneCh
		errorChPool.Put(chv)
		if nil == err {
			resp = task.Res
		}
		return
	default:
		var zero Res
		errorChPool.Put(chv)
		return zero, ErrBatcherOverflow
	}
}

// QueueLen returns the number of pending items, which weren't passed into
// BatcherFunc yet.
//
// Maximum number of pending items is GenericBatcherTask.QueueSize.
func (b *GenericBatcherTask[Req, Res]) QueueLen() int {
	return len(b.ch)
}

func processGenericBatchesTask[T any](f GenericBatcherTaskFunc[T], ch <-chan *T, maxBatchSize int, maxDelay time.Duration, semaphore chan struct{}) {
	var batch []*T
	var x *T
	var ok bool
	lastPushTime := time.Now()

	// Create the batch with the right capacity to avoid reallocations
	batch = make([]*T, 0, maxBatchSize)

	for {
		select {
		case x, ok = <-ch:
			if !ok {
				// When the channel is closed, process any remaining items
				if len(batch) > 0 {
					f(batch)
				}
				return
			}
			batch = append(batch, x)

			// If we've reached maxBatchSize, process immediately
			if len(batch) >= maxBatchSize {
				f(batch)
				batch = batch[:0]
				lastPushTime = time.Now()
			}
		default:
			if len(batch) == 0 {
				// Wait for at least one item
				x, ok = <-ch
				if !ok {
					return
				}
				batch = append(batch, x)
				lastPushTime = time.Now()
			} else {
				// Check if we've reached the timeout
				sinceLastPush := time.Since(lastPushTime)
				if sinceLastPush >= maxDelay {
					f(batch)
					batch = batch[:0]
					lastPushTime = time.Now()
				} else {
					// Wait for either a new item or the timeout
					t := acquireTimer(maxDelay - sinceLastPush)
					select {
					case x, ok = <-ch:
						if !ok {
							// Channel closed, process remaining items
							if len(batch) > 0 {
								f(batch)
							}
							releaseTimer(t)
							return
						}
						batch = append(batch, x)

						// If we've reached maxBatchSize, process immediately
						if len(batch) >= maxBatchSize {
							f(batch)
							batch = batch[:0]
							lastPushTime = time.Now()
						}
					case <-t.C:
						// Timeout reached, process the batch
						f(batch)
						batch = batch[:0]
						lastPushTime = time.Now()
					}
					releaseTimer(t)
				}
			}
		}
	}
}

func callGenericTask[T any](f GenericBatcherTaskFunc[T], batch []*T) {
	if len(batch) > 0 {
		f(batch)
	}
}

var errorChPool sync.Pool

// ErrBatcherOverflow is returned when the batcher queue is full
var ErrBatcherOverflow = error(batOverflow("batcher overflow"))

type batOverflow string

func (bo batOverflow) Error() string {
	return string(bo)
}
