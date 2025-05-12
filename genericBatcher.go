// Package batcher groups items in batches
// and calls the user-specified function on these batches.
package batcher

import (
	"time"
)

// GenericBatcher groups items in batches and calls Func on them.
//
// See also BytesBatcher.
type GenericBatcher[T any] struct {
	// Func is called by GenericBatcher when batch is ready to be processed.
	Func GenericBatcherFunc[T]

	// Maximum batch size that will be passed to BatcherFunc.
	MaxBatchSize int

	// Maximum delay between Push() and BatcherFunc call.
	MaxDelay time.Duration

	// Maximum unprocessed items' queue size.
	QueueSize int

	// MaxConcurrency limits the number of concurrent goroutines processing batches
	// Default is 100
	MaxConcurrency int

	ch        chan T
	doneCh    chan struct{}
	semaphore chan struct{}
}

// GenericBatcherFunc is called by GenericBatcher when batch is ready to be processed.
//
// GenericBatcherFunc must process the given batch before returning.
// It must not hold references to the batch after returning.
type GenericBatcherFunc[T any] func(batch []T)

// Start starts batch processing.
func (b *GenericBatcher[T]) Start() {
	if b.ch != nil {
		panic("batcher already started")
	}
	if b.Func == nil {
		panic("GenericBatcher.Func must be set")
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

	b.ch = make(chan T, b.QueueSize)
	b.doneCh = make(chan struct{})
	b.semaphore = make(chan struct{}, b.MaxConcurrency)

	go func() {
		processGenericBatches(b.Func, b.ch, b.MaxBatchSize, b.MaxDelay, b.semaphore)
		close(b.doneCh)
	}()
}

// Stop stops batch processing.
func (b *GenericBatcher[T]) Stop() {
	if b.ch == nil {
		panic("BUG: forgot calling GenericBatcher.Start()?")
	}
	close(b.ch)
	<-b.doneCh
	b.ch = nil
	b.doneCh = nil
	b.semaphore = nil
}

// Push pushes new item into the batcher.
// Don't forget calling Start() before pushing items into the batcher.
func (b *GenericBatcher[T]) Push(x T) bool {
	if b.ch == nil {
		panic("BUG: forgot calling GenericBatcher.Start()?")
	}
	select {
	case b.ch <- x:
		return true
	default:
		return false
	}
}

// PushGuaranteed pushes new item into the batcher.
// PushGuaranteed waits for the queue to be unblocked when pushing a new item
func (b *GenericBatcher[T]) PushGuaranteed(x T) {
	if b.ch == nil {
		panic("BUG: forgot calling GenericBatcher.Start()?")
	}
	b.ch <- x
}

// QueueLen returns the number of pending items, which weren't passed into
// BatcherFunc yet.
//
// Maximum number of pending items is GenericBatcher.QueueSize.
func (b *GenericBatcher[T]) QueueLen() int {
	return len(b.ch)
}

func processGenericBatches[T any](f GenericBatcherFunc[T], ch <-chan T, maxBatchSize int, maxDelay time.Duration, semaphore chan struct{}) {
	var batch []T
	var x T
	var ok bool
	lastPushTime := time.Now()

	// Create the batch with the right capacity to avoid reallocations
	batch = make([]T, 0, maxBatchSize)

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

func callGeneric[T any](f GenericBatcherFunc[T], batch []T) {
	if len(batch) > 0 {
		f(batch)
	}
}
