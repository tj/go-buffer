package buffer_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tj/assert"

	"github.com/tj/go-buffer"
)

// Test forced flush.
func TestBuffer_flush(t *testing.T) {
	var mu sync.Mutex
	var flushes int
	var flushed []interface{}

	flush := func(ctx context.Context, values []interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		flushes++
		flushed = append(flushed, values...)
		return nil
	}

	errors := func(err error) {
		assert.NoError(t, err)
	}

	b := buffer.New(
		buffer.WithFlushHandler(flush),
		buffer.WithErrorHandler(errors),
	)

	b.Push("hello")
	b.Push(" ")

	b.Flush()

	b.Push("world")
	b.Push("!")

	b.Close()

	assert.Equal(t, 2, flushes, "flush count")
	assert.Len(t, flushed, 4)
}

// Test max entries.
func TestBuffer_maxEntries(t *testing.T) {
	var mu sync.Mutex
	var flushes int
	var flushed []interface{}

	flush := func(ctx context.Context, values []interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		flushes++
		flushed = append(flushed, values...)
		return nil
	}

	errors := func(err error) {
		assert.NoError(t, err)
	}

	b := buffer.New(
		buffer.WithFlushHandler(flush),
		buffer.WithErrorHandler(errors),
		buffer.WithLimits(buffer.Limits{
			MaxEntries: 3,
		}),
	)

	b.Push("hello")
	b.Push(" ")
	b.Push("world")
	b.Push("!")

	b.Close()

	assert.Len(t, flushed, 4)
	assert.Equal(t, 2, flushes, "flush count")
}

// Test flush interval.
func TestBuffer_flushInterval(t *testing.T) {
	var mu sync.Mutex
	var flushes int
	var flushed []interface{}

	flush := func(ctx context.Context, values []interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		flushes++
		flushed = append(flushed, values...)
		return nil
	}

	errors := func(err error) {
		assert.NoError(t, err)
	}

	b := buffer.New(
		buffer.WithFlushHandler(flush),
		buffer.WithErrorHandler(errors),
		buffer.WithLimits(buffer.Limits{
			MaxEntries:    100,
			FlushInterval: time.Millisecond * 50,
		}),
	)

	b.Push("hello")
	time.Sleep(time.Millisecond * 150)

	b.Push(" ")
	time.Sleep(time.Millisecond * 150)

	b.Push("world")
	b.Push("!")

	b.Close()

	assert.Len(t, flushed, 4)
	assert.Equal(t, 3, flushes, "flush count")
}

// temporaryError .
type temporaryError struct{}

// Error implementation.
func (e temporaryError) Error() string {
	return "rate limited"
}

// Temporary implementation.
func (e temporaryError) Temporary() bool {
	return true
}

// Test flush retries on temporary errors.
func TestBuffer_flushRetriesOk(t *testing.T) {
	var mu sync.Mutex
	var flushes int

	flush := func(ctx context.Context, values []interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		flushes++
		if flushes < 3 {
			return temporaryError{}
		}
		return nil
	}

	errors := func(err error) {
		assert.NoError(t, err)
	}

	b := buffer.New(
		buffer.WithFlushHandler(flush),
		buffer.WithErrorHandler(errors),
	)

	b.Push("hello")
	b.Push("world")
	b.Push("!")

	b.Close()

	assert.Equal(t, 3, flushes, "flush count")
}

// Test exceeding retries.
func TestBuffer_flushRetriesExceeded(t *testing.T) {
	var mu sync.Mutex
	var flushes int
	var err error

	flush := func(ctx context.Context, values []interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		flushes++
		return temporaryError{}
	}

	errors := func(e error) {
		err = e
	}

	b := buffer.New(
		buffer.WithFlushHandler(flush),
		buffer.WithErrorHandler(errors),
	)

	b.Push("hello")
	b.Push("world")
	b.Push("!")

	b.Close()

	assert.Equal(t, 3, flushes, "flush count")
	assert.EqualError(t, err, `rate limited`)
}

// Test regular flush errors.
func TestBuffer_flushErrors(t *testing.T) {
	var mu sync.Mutex
	var flushes int
	var err error

	flush := func(ctx context.Context, values []interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		flushes++
		return fmt.Errorf("boom")
	}

	errors := func(e error) {
		err = e
	}

	b := buffer.New(
		buffer.WithFlushHandler(flush),
		buffer.WithErrorHandler(errors),
	)

	b.Push("hello")
	b.Push("world")
	b.Push("!")

	b.Close()

	assert.Equal(t, 1, flushes, "flush count")
	assert.EqualError(t, err, `boom`)
}

// Benchmark pushing.
func BenchmarkPush(b *testing.B) {
	buf := buffer.New(
		buffer.WithLimits(buffer.Limits{
			MaxEntries:    250,
			FlushInterval: time.Second,
		}),
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Push("hello")
	}

	buf.Close()
}
