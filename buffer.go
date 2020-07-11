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

// DefaultLimits is the set of default limits used for flushing.
var DefaultLimits = Limits{
	MaxEntries:    250,
	MaxRetries:    3,
	FlushInterval: time.Second * 5,
	FlushTimeout:  time.Second * 15,
}

// Limits configuration.
type Limits struct {
	// MaxEntries is the maximum number of entries before the buffer is flushed.
	MaxEntries int

	// MaxRetries is the maximum number of retries to perform on temporary errors.
	MaxRetries int

	// FlushInterval is the interval used to periodically flush the entries. Zero disables this feature.
	FlushInterval time.Duration

	// FlushTimeout is the timeout used in the context for flushing entries.
	FlushTimeout time.Duration
}

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
	v.limits = DefaultLimits
	v.handleFlush = noopFlush
	v.handleError = noopError

	for _, o := range options {
		o(&v)
	}

	v.done = make(chan struct{})
	if v.limits.FlushInterval > 0 {
		go v.intervalFlush()
	}

	return &v
}

// WithFlushHandler registers the function handling flushes.
func WithFlushHandler(fn FlushFunc) Option {
	return func(v *Buffer) {
		v.handleFlush = fn
	}
}

// WithErrorHandler registers the function handling errors.
func WithErrorHandler(fn ErrorFunc) Option {
	return func(v *Buffer) {
		v.handleError = fn
	}
}

// WithLimits sets the limits used for flushing.
func WithLimits(limits Limits) Option {
	return func(v *Buffer) {
		v.limits = limits
	}
}

// Buffer is used to batch entries.
type Buffer struct {
	handleFlush    FlushFunc
	handleError    ErrorFunc
	limits         Limits
	pendingFlushes sync.WaitGroup
	done           chan struct{}

	mu     sync.Mutex
	values []interface{}
}

// Push adds a value to the buffer.
func (b *Buffer) Push(value interface{}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.values = append(b.values, value)

	if len(b.values) >= b.limits.MaxEntries {
		b.flush()
	}
}

// Flush flushes any pending entries.
func (b *Buffer) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.flush()
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
	tick := time.NewTicker(b.limits.FlushInterval)
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
	ctx, cancel := context.WithTimeout(context.Background(), b.limits.FlushTimeout)

	err := b.handleFlush(ctx, values)
	cancel()

	// temporary error, retry if we haven't exceeded MaxEntries
	if e, ok := err.(temporary); ok && e.Temporary() {
		logs.Printf("temporary error flushing %d entries: %v", len(values), e)
		time.Sleep(time.Second) // TODO: backoff
		retries++
		if retries < b.limits.MaxRetries {
			goto retry
		}
		logs.Printf("max retries of %d exceeded", b.limits.MaxRetries)
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
