package batcher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGenericBatcherStartStop(t *testing.T) {
	b := &GenericBatcher[interface{}]{
		Func: func(batch []interface{}) {},
	}
	for i := 0; i < 100; i++ {
		b.Start()
		b.Stop()
	}
}

func TestGenericBatcherPushNotStarted(t *testing.T) {
	b := &GenericBatcher[interface{}]{
		Func: func(batch []interface{}) {},
	}
	expectPanic(t, func() { b.Push(123) })
}

func TestGenericBatcherStopNotStarted(t *testing.T) {
	b := &GenericBatcher[interface{}]{
		Func: func(batch []interface{}) {},
	}
	expectPanic(t, func() { b.Stop() })
}

func TestGenericBatcherDoubleStop(t *testing.T) {
	b := &GenericBatcher[interface{}]{
		Func: func(batch []interface{}) {},
	}
	b.Start()
	b.Stop()
	expectPanic(t, func() { b.Stop() })
}

func TestGenericBatcherDoubleStart(t *testing.T) {
	b := &GenericBatcher[interface{}]{
		Func: func(batch []interface{}) {},
	}
	b.Start()
	expectPanic(t, func() { b.Start() })
}

func TestGenericBatcherPushStop(t *testing.T) {
	n := 0
	b := &GenericBatcher[interface{}]{
		Func:     func(batch []interface{}) { n += len(batch) },
		MaxDelay: time.Hour,
	}
	b.Start()
	for i := 0; i < 10; i++ {
		if !b.Push(i) {
			t.Fatalf("cannot add item %d to batch", i)
		}
	}
	b.Stop()

	if n != 10 {
		t.Fatalf("Unexpected n=%d. Expected 10", n)
	}
}

func TestGenericBatcherPushMaxBatchSize(t *testing.T) {
	testGenericBatcherPushMaxBatchSize(t, 1, 100)
	testGenericBatcherPushMaxBatchSize(t, 10, 100)
	testGenericBatcherPushMaxBatchSize(t, 100, 100)
	testGenericBatcherPushMaxBatchSize(t, 101, 100)
	testGenericBatcherPushMaxBatchSize(t, 1003, 15)
	testGenericBatcherPushMaxBatchSize(t, 1033, 17)
}

func TestGenericBatcherPushMaxDelay(t *testing.T) {
	testGenericBatcherPushMaxDelay(t, 100, time.Millisecond)
	testGenericBatcherPushMaxDelay(t, 205, 10*time.Millisecond)
	testGenericBatcherPushMaxDelay(t, 313, 100*time.Millisecond)
}

func TestGenericBatcherConcurrentPush(t *testing.T) {
	s := uint32(0)
	b := &GenericBatcher[interface{}]{
		Func: func(batch []interface{}) {
			for _, v := range batch {
				s += v.(uint32)
			}
		},
	}
	b.Start()
	var wg sync.WaitGroup
	ss := uint32(0)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				b.Push(uint32(i))
				time.Sleep(time.Millisecond)
				atomic.AddUint32(&ss, uint32(i))
			}
			wg.Done()
		}()
	}
	wg.Wait()
	b.Stop()
	if s != ss {
		t.Fatalf("Unepxected sum %d. Expecting %d", s, ss)
	}
}

func TestGenericBatcherQueueSize(t *testing.T) {
	ch := make(chan struct{})
	n := 0
	b := &GenericBatcher[interface{}]{
		Func: func(batch []interface{}) {
			<-ch
			n += len(batch)
		},
		MaxDelay:     time.Hour,
		MaxBatchSize: 3,
		QueueSize:    10,
	}
	b.Start()
	for i := 0; i < 3; i++ {
		if !b.Push(i) {
			t.Fatalf("cannot add item %d to batch", i)
		}
	}
	time.Sleep(time.Millisecond)
	for i := 0; i < 10; i++ {
		if !b.Push(i) {
			t.Fatalf("cannot add item %d to batch", i)
		}
	}
	if b.QueueLen() != b.QueueSize {
		t.Fatalf("Unexpected queue size %d. Expecting %d", b.QueueLen(), b.QueueSize)
	}
	for i := 0; i < 10; i++ {
		if b.Push(123) {
			t.Fatalf("expecting queue overflow")
		}
		time.Sleep(time.Millisecond)
	}
	close(ch)
	time.Sleep(time.Millisecond)
	for i := 0; i < 5; i++ {
		if !b.Push(i) {
			t.Fatalf("cannot add item %d to batch", i)
		}
	}
	b.Stop()

	if n != 18 {
		t.Fatalf("Unexpected number of items passed to batcher func: %d. Expected 18", n)
	}
}

func testGenericBatcherPushMaxDelay(t *testing.T, itemsCount int, maxDelay time.Duration) {
	lastTime := time.Now()
	n := 0
	nn := 0
	b := &GenericBatcher[interface{}]{
		Func: func(batch []interface{}) {
			if time.Since(lastTime) > maxDelay+10*time.Millisecond {
				t.Fatalf("Unexpected delay between batches: %s. Expected no more than %s. itemsCount=%d",
					time.Since(lastTime), maxDelay, itemsCount)
			}
			lastTime = time.Now()
			nn += len(batch)
			n++
		},
		MaxDelay:     maxDelay,
		MaxBatchSize: 100500,
	}
	b.Start()
	for i := 0; i < itemsCount; i++ {
		if !b.Push(i) {
			t.Fatalf("cannot add item %d to batch", i)
		}
		time.Sleep(time.Millisecond)
	}
	b.Stop()

	batchSize := 1000 * maxDelay.Seconds()
	expectedN := int(1.2 * (float64(itemsCount) + batchSize - 1) / batchSize)
	if n > expectedN {
		t.Fatalf("Unexpected number of batch func calls: %d. Expected no more than %d. itemsCount=%d, maxDelay=%s",
			n, expectedN, itemsCount, maxDelay)
	}
	if itemsCount != nn {
		t.Fatalf("Unexpected number of items passed to batcher func: %d. Expected %d. maxDelay=%s", nn, itemsCount, maxDelay)
	}
}

func testGenericBatcherPushMaxBatchSize(t *testing.T, itemsCount, batchSize int) {
	n := 0
	nn := 0
	b := &GenericBatcher[interface{}]{
		Func: func(batch []interface{}) {
			if len(batch) > batchSize {
				t.Fatalf("Unexpected batch size=%d. Must not exceed %d. itemsCount=%d", len(batch), batchSize, itemsCount)
			}
			if len(batch) == 0 {
				t.Fatalf("Empty batch. itemsCount=%d, batchSize=%d", itemsCount, batchSize)
			}
			nn += len(batch)
			n++
		},
		MaxDelay:     time.Hour,
		MaxBatchSize: batchSize,
	}
	b.Start()
	for i := 0; i < itemsCount; i++ {
		if !b.Push(i) {
			t.Fatalf("cannot add item %d to batch", i)
		}
	}
	b.Stop()

	expectedN := (itemsCount + batchSize - 1) / batchSize
	if n != expectedN {
		t.Fatalf("Unexpected number of batcher func calls: %d. Expected %d. itemsCount=%d, batchSize=%d",
			n, expectedN, itemsCount, batchSize)
	}
	if nn != itemsCount {
		t.Fatalf("Unexpected number of items in all batches: %d. Expected %d. batchSize=%d", nn, itemsCount, batchSize)
	}
}
