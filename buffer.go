// Package buffer provides a generic buffer or batching mechanism for flushing
// entries at a given size or interval, useful for cases such as batching log
// events.
package buffer

import (
	"context"
	"log"
	"os"
	"sync"
	"time"
)

// TODO: make logging optional, accept a Printfer

// logs instance.
var logs = log.New(os.Stderr, "buffer ", log.LstdFlags)

// temporary is the interfaced used for temporary errors.
type temporary interface {
	Temporary() bool
}

// DefaultMaxEntries is the default max entries limit.
var DefaultMaxEntries = 250

// DefaultMaxRetries is the default max retries limit.
var DefaultMaxRetries = 3

// DefaultFlushInterval is the default flush interval.
var DefaultFlushInterval = time.Second * 5

// DefaultFlushTimeout is the default flush timeout.
var DefaultFlushTimeout = time.Second * 15

// FlushFunc is the flush callback function used to flush entries.
type FlushFunc func(context.Context, []interface{}) error

// ErrorFunc is the error callback function used to report flushing errors,
// called when flushing fails and MaxRetries has exceeded on temporary errors.
type ErrorFunc func(error)

// Option function.
type Option func(*Buffer)

// New buffer with the given options.
func New(options ...Option) *Buffer {
	var v Buffer
	v.handleFlush = noopFlush
	v.handleError = noopError
	v.maxEntries = DefaultMaxEntries
	v.maxRetries = DefaultMaxRetries
	v.flushInterval = DefaultFlushInterval
	v.flushTimeout = DefaultFlushTimeout

	for _, o := range options {
		o(&v)
	}

	v.done = make(chan struct{})
	if v.flushInterval > 0 {
		go v.intervalFlush()
	}

	return &v
}

// WithFlushHandler sets the function handling flushes.
func WithFlushHandler(fn FlushFunc) Option {
	return func(v *Buffer) {
		v.handleFlush = fn
	}
}

// WithErrorHandler sets the function handling errors.
func WithErrorHandler(fn ErrorFunc) Option {
	return func(v *Buffer) {
		v.handleError = fn
	}
}

// WithMaxEntries sets the maximum number of entries before flushing.
func WithMaxEntries(n int) Option {
	return func(v *Buffer) {
		v.maxEntries = n
	}
}

// WithMaxRetries sets the maximum number of retries for temporary flush errors.
func WithMaxRetries(n int) Option {
	return func(v *Buffer) {
		v.maxRetries = n
	}
}

// WithFlushInterval sets the interval at which events are periodically flushed.
func WithFlushInterval(d time.Duration) Option {
	return func(v *Buffer) {
		v.flushInterval = d
	}
}

// WithFlushTimeout sets the flush timeout.
func WithFlushTimeout(d time.Duration) Option {
	return func(v *Buffer) {
		v.flushTimeout = d
	}
}

// Buffer is used to batch entries.
type Buffer struct {
	pendingFlushes sync.WaitGroup
	done           chan struct{}

	// callbacks
	handleFlush FlushFunc
	handleError ErrorFunc

	// limits
	maxEntries    int
	maxRetries    int
	flushInterval time.Duration
	flushTimeout  time.Duration

	// buffer
	mu     sync.Mutex
	values []interface{}
}

// Push adds a value to the buffer.
func (b *Buffer) Push(value interface{}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.values = append(b.values, value)

	if len(b.values) >= b.maxEntries {
		b.flush()
	}
}

// Flush flushes any pending entries asynchronously.
func (b *Buffer) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.flush()
}

// FlushSync flushes any pending entries synchronously.
func (b *Buffer) FlushSync() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.doFlush(b.values)
	b.values = nil
}

// Close flushes any pending entries, and waits for flushing to complete. This
// method should be called before exiting your program to ensure entries have
// flushed properly.
func (b *Buffer) Close() {
	b.Flush()
	close(b.done)
	b.pendingFlushes.Wait()
}

// intervalFlush starts a loop flushing at the FlushInterval.
func (b *Buffer) intervalFlush() {
	tick := time.NewTicker(b.flushInterval)
	for {
		select {
		case <-tick.C:
			b.Flush()
		case <-b.done:
			tick.Stop()
			return
		}
	}
}

// flush performs a flush asynchronuously.
func (b *Buffer) flush() {
	values := b.values
	b.values = nil

	if len(values) == 0 {
		return
	}

	b.pendingFlushes.Add(1)

	go func() {
		b.doFlush(values)
		b.pendingFlushes.Done()
	}()
}

// doFlush handles flushing of the given values.
func (b *Buffer) doFlush(values []interface{}) {
	var retries int

retry:
	ctx, cancel := context.WithTimeout(context.Background(), b.flushTimeout)

	err := b.handleFlush(ctx, values)
	cancel()

	// temporary error, retry if we haven't exceeded MaxEntries
	if e, ok := err.(temporary); ok && e.Temporary() {
		logs.Printf("temporary error flushing %d entries: %v", len(values), e)
		time.Sleep(time.Second) // TODO: backoff
		retries++
		if retries < b.maxRetries {
			goto retry
		}
		logs.Printf("max retries of %d exceeded", b.maxRetries)
	}

	if err != nil {
		b.handleError(err)
	}
}

// noopFlush function.
func noopFlush(context.Context, []interface{}) error {
	return nil
}

// noopError function.
func noopError(err error) {
	logs.Printf("unhandled error: %v", "")
}
