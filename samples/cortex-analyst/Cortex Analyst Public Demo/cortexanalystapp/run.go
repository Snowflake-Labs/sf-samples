package cortexanalystapp

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Waiter helps spawn and wait for goroutines, running functions that might return errors.
// This is like errgroup.Group, but differs in that all spawned goroutines are run to completion,
// rather than failing on the first error.
type Waiter struct {
	wg   sync.WaitGroup
	errs []error
}

// Wait waits for the spawned goroutines to return.  Returns the errors from the goroutine funcs.
func (w *Waiter) Wait() error {
	w.wg.Wait()
	return errors.Join(w.errs...)
}

// Go spawns separate goroutines for each func in funcs.
func (w *Waiter) Go(funcs ...func() error) {
	for _, f := range funcs {
		w.wg.Add(1)
		go func(f func() error) {
			defer w.wg.Done()
			w.errs = append(w.errs, f())
		}(f)
	}
}

// GoNoError spawns separate goroutines for each func in funcs.
func (w *Waiter) GoNoError(funcs ...func()) {
	for _, f := range funcs {
		w.wg.Add(1)
		go func(f func()) {
			defer w.wg.Done()
			f()
		}(f)
	}
}

// Go spawns separate goroutines for each func in funcs.
// Returns a new Waiter that can be used to wait for the goroutines to finish.
func Go(funcs ...func() error) *Waiter {
	w := &Waiter{}
	w.Go(funcs...)
	return w
}

// GoNoError spawns separate goroutines for each func in funcs.
// Returns a new Waiter that can be used to wait for the goroutines to finish.
func GoNoError(funcs ...func()) *Waiter {
	w := &Waiter{}
	w.GoNoError(funcs...)
	return w
}

// SleepWithContext returns true after sleeping for d duration, or false if ctx is done.
// Guarantees to return false and not sleep if ctx is done before the call.
// Otherwise guarantees to return true and not sleep if d <= 0.
func SleepWithContext(ctx context.Context, d time.Duration) bool {
	switch {
	case ctx.Err() != nil:
		return false // ctx is done, guarantee return false and no sleep.
	case d <= 0:
		return true // guarantee return true and no sleep.
	}
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return false
	case <-t.C:
		return true
	}
}

// Loop repeatedly calls f until f returns a non-nil error or ctx is done.
// Returns nil iff ctx is done, otherwise returns the non-nil error from f.
// Guarantees at least 1 call to f.
func Loop(ctx context.Context, f func() error) error {
	for {
		if err := f(); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil // ctx is done.
		}
	}
}

// LoopEvery repeatedly calls f with the given period, until f returns a non-nil error or ctx is done.
// Returns nil iff ctx is done, otherwise returns the non-nil error from f.
//
// Guarantees at least 1 call to f; the initial call occurs immediately, and subsequent calls occur
// with at least period delay, including the time it takes to run f.  If f takes longer than period,
// or period <= 0, there is no artificial delay between calls to f.
func LoopEvery(ctx context.Context, period time.Duration, f func() error) error {
	return LoopEveryWithJitter(ctx, period, 0, f)
}

// LoopEveryWithJitter is like LoopEvery, but adds a random jitter to the period.
// The jitter is specified as a fraction of the period. The effective sleep is uniformly randomly sampled from
// the interval [period - jitterFrac*period, period + jitterFrac*period].
func LoopEveryWithJitter(ctx context.Context, period time.Duration, jitterFrac float64, f func() error) error {
	if period <= 0 {
		return Loop(ctx, f)
	}
	if jitterFrac < 0 || jitterFrac > 1 {
		return fmt.Errorf("invalid jitter fraction %f; must be in the range [0, 1]", jitterFrac)
	}
	for {
		start := time.Now()
		effectivePeriod := period
		if jitterFrac > 0 {
			effectivePeriod = period + time.Duration(jitterFrac*float64(period)*(rand.Float64()*2-1))
		}
		if err := f(); err != nil {
			return err
		}
		if !SleepWithContext(ctx, effectivePeriod-time.Since(start)) {
			return nil // ctx is done.
		}
	}
}

// LoopSleep repeatedly calls f and sleeps for sleep, until f returns a non-nil error or ctx is done.
// Returns nil iff ctx is done, otherwise returns the non-nil error from f.
//
// Guarantees at least 1 call to f; the initial call occurs immediately.  Successful calls to f are
// followed by a sleep for sleep, regardless of the time it takes to run f.  If sleep <= 0, there is
// no sleep between calls to f.
func LoopSleep(ctx context.Context, sleep time.Duration, f func() error) error {
	if sleep <= 0 {
		return Loop(ctx, f)
	}
	for {
		if err := f(); err != nil {
			return err
		}
		if !SleepWithContext(ctx, sleep) {
			return nil // ctx is done.
		}
	}
}

// GoLoop spawns a goroutine that calls Loop; see Loop for semantics.
// Returns a new Waiter that is used to wait for the goroutine to finish.
func GoLoop(ctx context.Context, f func() error) *Waiter {
	return Go(func() error { return Loop(ctx, f) })
}

// GoLoopEvery spawns a goroutine that calls LoopEvery; see LoopEvery for semantics.
// Returns a new Waiter that is used to wait for the goroutine to finish.
func GoLoopEvery(ctx context.Context, period time.Duration, f func() error) *Waiter {
	return Go(func() error { return LoopEvery(ctx, period, f) })
}
